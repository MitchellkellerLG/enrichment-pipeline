"""
Contact List Handler

Handles contact lists (multiple contacts per company) by:
1. Detecting if input is a contact list
2. Deduplicating by company
3. Enriching unique companies
4. Joining enrichment back to contacts
5. Pushing webhook per contact
"""

import pandas as pd
from typing import Optional
import asyncio
import requests
from datetime import datetime


# Contact identifier columns (lowercase for matching)
CONTACT_COLUMNS = [
    'email', 'email_address', 'contact_email',
    'linkedin_url', 'linkedin_profile', 'linkedin',
    'contact_id', 'person_id', 'lead_id',
    'first_name', 'last_name'
]


def detect_input_type(df: pd.DataFrame) -> str:
    """
    Detect if input is a contact list or company list.

    Contact lists have personal identifiers (email, linkedin_url, first_name).
    Company lists only have company_name and website.

    Args:
        df: Input DataFrame

    Returns:
        "contact_list" or "company_list"
    """
    df_cols_lower = [c.lower() for c in df.columns]
    has_contact_cols = any(
        col in df_cols_lower for col in CONTACT_COLUMNS
    )
    return "contact_list" if has_contact_cols else "company_list"


def extract_contact_columns(df: pd.DataFrame) -> list[str]:
    """
    Get list of contact-specific columns in the DataFrame.

    Args:
        df: Input DataFrame

    Returns:
        List of column names that are contact identifiers
    """
    df_cols_lower = {c.lower(): c for c in df.columns}
    found = []
    for col in CONTACT_COLUMNS:
        if col in df_cols_lower:
            found.append(df_cols_lower[col])
    return found


def dedupe_by_company(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract unique companies from a contact list.

    Args:
        df: Contact list DataFrame with company_name and website columns

    Returns:
        Tuple of (unique_companies_df, original_df_with_index)
        - unique_companies_df has deduplicated company_name, website
        - original_df has added _contact_index column for later join
    """
    # Add index for later join
    df = df.copy()
    df['_contact_index'] = range(len(df))

    # Extract unique companies
    unique_companies = df[['company_name', 'website']].drop_duplicates()

    print(f"Contact list: {len(df)} contacts from {len(unique_companies)} unique companies")

    return unique_companies, df


def join_enrichment_to_contacts(
    contacts_df: pd.DataFrame,
    enrichment_data: dict,
    company_key_fn
) -> pd.DataFrame:
    """
    Join enrichment results back to original contact list.

    Args:
        contacts_df: Original contact DataFrame with _contact_index
        enrichment_data: Dict mapping company_key to enrichment result
        company_key_fn: Function to create company key from row

    Returns:
        DataFrame with enrichment data merged in
    """
    def get_enrichment(row):
        key = company_key_fn({
            'company_name': row.get('company_name', ''),
            'website': row.get('website', '')
        })
        return enrichment_data.get(key, {})

    # Add enrichment as nested dict column
    contacts_df = contacts_df.copy()
    contacts_df['_enrichment'] = contacts_df.apply(get_enrichment, axis=1)

    return contacts_df


def build_contact_webhook_record(
    contact_row: pd.Series,
    enrichment: dict,
    run_id: str
) -> dict:
    """
    Build webhook record for a single contact with company enrichment.

    Args:
        contact_row: Single row from contacts DataFrame
        enrichment: Enrichment dict with icp, careers, news objects
        run_id: Pipeline run ID

    Returns:
        Webhook record with contact info + company enrichment
    """
    # Extract contact-specific fields
    contact_fields = {}
    for col in CONTACT_COLUMNS:
        # Check various casings
        for df_col in contact_row.index:
            if df_col.lower() == col:
                val = contact_row[df_col]
                if pd.notna(val):
                    contact_fields[col] = val
                break

    record = {
        "run_id": run_id,
        "processed_at": datetime.now().isoformat(),

        # Contact info nested
        "contact": contact_fields,

        # Company identifiers
        "company_name": contact_row.get('company_name'),
        "website": contact_row.get('website'),

        # Enrichment data (already nested objects)
        "mx": enrichment.get('mx'),
        "icp": enrichment.get('icp'),
        "careers": enrichment.get('careers'),
        "news": enrichment.get('news'),
    }

    return record


async def push_contacts_to_webhook(
    contacts_df: pd.DataFrame,
    enrichment_data: dict,
    company_key_fn,
    webhook_url: str,
    run_id: str,
    semaphore,
    dry_run: bool = False,
    verbose: bool = False
) -> dict:
    """
    Push webhook for each contact with company enrichment attached.

    Args:
        contacts_df: Contact DataFrame with _contact_index
        enrichment_data: Dict mapping company_key to enrichment result
        company_key_fn: Function to create company key
        webhook_url: Destination webhook URL
        run_id: Pipeline run ID
        semaphore: Asyncio semaphore for rate limiting
        dry_run: If True, skip actual webhook push
        verbose: Print progress

    Returns:
        Stats dict with success/failure counts
    """
    stats = {
        "total": len(contacts_df),
        "pushed": 0,
        "failed": 0,
        "skipped": 0
    }

    async def push_single(idx, row):
        async with semaphore:
            key = company_key_fn({
                'company_name': row.get('company_name', ''),
                'website': row.get('website', '')
            })
            enrichment = enrichment_data.get(key, {})

            record = build_contact_webhook_record(row, enrichment, run_id)

            if dry_run:
                stats["skipped"] += 1
                if verbose:
                    print(f"  [DRY RUN] Contact {idx}: {record.get('contact', {}).get('email', 'unknown')}")
                return

            try:
                response = requests.post(
                    webhook_url,
                    json=[record],
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )
                response.raise_for_status()
                stats["pushed"] += 1
                if verbose:
                    print(f"  [{stats['pushed']}/{stats['total']}] Pushed: {record.get('contact', {}).get('email', 'unknown')}")
            except Exception as e:
                stats["failed"] += 1
                if verbose:
                    print(f"  [FAILED] Contact {idx}: {e}")

    # Push all contacts
    tasks = [
        push_single(idx, row)
        for idx, row in contacts_df.iterrows()
    ]
    await asyncio.gather(*tasks)

    return stats


class ContactListProcessor:
    """
    High-level processor for contact lists.

    Usage:
        processor = ContactListProcessor(state_store, cache_store, semaphores)
        result = await processor.process(contacts_df, run_id, webhook_url)
    """

    def __init__(self, state_store, cache_store, semaphores, company_key_fn):
        self.state_store = state_store
        self.cache_store = cache_store
        self.semaphores = semaphores
        self.company_key_fn = company_key_fn

    async def process(
        self,
        contacts_df: pd.DataFrame,
        run_id: str,
        dag_scheduler_fn,
        webhook_url: str = None,
        dry_run: bool = False,
        verbose: bool = False
    ) -> dict:
        """
        Process a contact list: dedupe -> enrich -> join -> push.

        Args:
            contacts_df: Input contact DataFrame
            run_id: Pipeline run ID
            dag_scheduler_fn: The DAG scheduler function to call for company enrichment
            webhook_url: Webhook destination
            dry_run: Skip webhook pushes
            verbose: Print progress

        Returns:
            Processing stats
        """
        print(f"\n{'='*60}")
        print("CONTACT LIST PROCESSOR")
        print(f"{'='*60}")

        # Step 1: Dedupe by company
        unique_companies, contacts_with_index = dedupe_by_company(contacts_df)
        companies = unique_companies.to_dict('records')

        print(f"Enriching {len(companies)} unique companies...")

        # Step 2: Enrich companies using DAG scheduler
        await dag_scheduler_fn(
            companies=companies,
            run_id=run_id,
            state_store=self.state_store,
            cache_store=self.cache_store,
            semaphores=self.semaphores,
            webhook_url=None,  # Don't push company webhooks
            dry_run=True,  # Don't push during enrichment
            verbose=verbose
        )

        # Step 3: Collect enrichment data
        enrichment_data = {}
        for company in companies:
            key = self.company_key_fn(company)
            # Get enrichment from state store
            node_states = self.state_store.get_all_node_states(run_id, key)
            enrichment_data[key] = self._build_enrichment_dict(node_states)

        # Step 4: Push per contact
        print(f"\nPushing webhooks for {len(contacts_with_index)} contacts...")

        webhook_semaphore = asyncio.Semaphore(10)  # Limit concurrent webhook calls
        push_stats = await push_contacts_to_webhook(
            contacts_with_index,
            enrichment_data,
            self.company_key_fn,
            webhook_url,
            run_id,
            webhook_semaphore,
            dry_run=dry_run,
            verbose=verbose
        )

        print(f"\n{'='*60}")
        print("CONTACT LIST COMPLETE")
        print(f"{'='*60}")
        print(f"Total Contacts:     {push_stats['total']}")
        print(f"Webhooks Pushed:    {push_stats['pushed']}")
        print(f"Failed:             {push_stats['failed']}")
        print(f"Skipped (dry run):  {push_stats['skipped']}")
        print(f"{'='*60}")

        return {
            "contacts_total": push_stats['total'],
            "companies_enriched": len(companies),
            "webhooks_pushed": push_stats['pushed'],
            "webhooks_failed": push_stats['failed']
        }

    def _build_enrichment_dict(self, node_states: dict) -> dict:
        """Build enrichment dict from node states."""
        result = {
            "mx": None,
            "icp": None,
            "careers": None,
            "news": None
        }

        # MX
        mx_state = node_states.get("mx_check", {})
        if mx_state and mx_state.get("output"):
            mx = mx_state["output"]
            result["mx"] = {
                "valid": mx.get("mx_valid", False),
                "records": mx.get("mx_records", []),
                "domain": mx.get("domain")
            }

        # ICP
        icp_state = node_states.get("icp_check", {})
        if icp_state and icp_state.get("output"):
            icp = icp_state["output"]
            is_b2b = icp.get("is_b2b")
            result["icp"] = {
                "is_qualified": is_b2b,
                "qualification_tier": "QUALIFIED" if is_b2b else ("DISQUALIFIED" if is_b2b is False else "NEEDS_REVIEW"),
                "confidence": icp.get("confidence"),
                "what_they_do": icp.get("what_they_do"),
                "who_they_serve": icp.get("who_they_serve"),
                "summary": icp.get("three_sentence_summary"),
                "primary_industry": icp.get("primary_industry"),
                "services_list": icp.get("services_list", []),
                "industries_served": icp.get("industries_served", []),
                "buyer_personas": icp.get("buyer_personas", []),
                "problems_solved": icp.get("problems_they_solve", []),
                "disqualify_reason": icp.get("disqualify_reason")
            }

        # Careers
        careers_state = node_states.get("careers", {})
        if careers_state and careers_state.get("output"):
            careers = careers_state["output"]
            result["careers"] = {
                "is_hiring": careers.get("is_hiring"),
                "hiring_intensity": careers.get("hiring_intensity"),
                "open_positions_count": careers.get("open_positions_count"),
                "careers_url": careers.get("careers_url"),
                "linkedin_url": careers.get("linkedin_url"),
                "job_titles": careers.get("all_job_titles", []),
                "matched_roles": careers.get("matched_roles", [])
            }

        # News
        news_state = node_states.get("news", {})
        if news_state and news_state.get("output"):
            news = news_state["output"]
            result["news"] = {
                "has_recent_news": news.get("has_recent_news"),
                "outreach_timing": news.get("outreach_timing"),
                "funding": {
                    "has_funding": news.get("has_funding_news"),
                    "round_type": news.get("funding_round_type"),
                    "amount": news.get("funding_amount"),
                    "investors": news.get("funding_investors", [])
                },
                "recent_announcements": news.get("recent_announcements", [])
            }

        return result
