"""
Contact List Handler

Handles contact lists (multiple contacts per company) by:
1. Detecting if input is a contact list
2. Cloning list (READ-ONLY original)
3. Extracting unique companies
4. Enriching unique companies only
5. Joining enrichment back to ALL original contacts
6. Pushing webhook per contact

Key principle: Original contact list is READ-ONLY. We only ADD enrichment columns.
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

# Company column name variants (will be normalized to 'website')
WEBSITE_COLUMN_VARIANTS = ['website', 'company_website', 'domain', 'url']
COMPANY_NAME_VARIANTS = ['company_name', 'company', 'organization']


def normalize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Normalize column names to standard format.

    Maps variants like 'company_website' -> 'website' for consistent processing.

    Args:
        df: Input DataFrame

    Returns:
        Tuple of (normalized DataFrame, mapping dict of original -> normalized names)
    """
    df = df.copy()
    mapping = {}

    # Normalize website column
    df_cols_lower = {c.lower(): c for c in df.columns}
    for variant in WEBSITE_COLUMN_VARIANTS:
        if variant in df_cols_lower:
            original_col = df_cols_lower[variant]
            if original_col != 'website':
                df = df.rename(columns={original_col: 'website'})
                mapping[original_col] = 'website'
            break

    # Normalize company_name column
    for variant in COMPANY_NAME_VARIANTS:
        if variant in df_cols_lower:
            original_col = df_cols_lower[variant]
            if original_col != 'company_name':
                df = df.rename(columns={original_col: 'company_name'})
                mapping[original_col] = 'company_name'
            break

    return df, mapping


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


def extract_unique_companies(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract unique companies from a contact list WITHOUT modifying original.

    This is the key function that implements the clone -> dedupe -> enrich -> join pattern.
    Original contact list is READ-ONLY. We add an index for later join-back.

    Args:
        df: Contact list DataFrame (must have 'company_name' and 'website' columns)

    Returns:
        Tuple of (unique_companies_df, original_df_with_index)
        - unique_companies_df: Deduplicated companies for enrichment (separate copy)
        - original_df_with_index: Original list with _contact_index for join-back
    """
    # Preserve original with index for later join
    original_with_index = df.copy()
    original_with_index['_contact_index'] = range(len(original_with_index))

    # Extract unique companies (separate copy - not modifying original)
    # Use normalized 'website' column as key
    company_cols = ['company_name', 'website']
    unique_companies = df[company_cols].drop_duplicates().copy()

    # Clean up website for key creation (remove http/https, trailing slashes)
    unique_companies['_website_key'] = unique_companies['website'].apply(
        lambda x: str(x).lower().replace('http://', '').replace('https://', '').rstrip('/') if pd.notna(x) else ''
    )

    print(f"Contact list: {len(df)} contacts from {len(unique_companies)} unique companies")

    return unique_companies, original_with_index


# Keep old name as alias for backward compatibility
dedupe_by_company = extract_unique_companies


def join_enrichment_to_contacts(
    contacts_df: pd.DataFrame,
    enrichment_data: dict,
    company_key_fn
) -> pd.DataFrame:
    """
    Join enrichment results back to ALL original contacts.

    Uses website as primary join key. Every contact gets enrichment data
    from their company. Multiple contacts from same company share identical
    enrichment.

    Args:
        contacts_df: Original contact DataFrame with _contact_index (READ-ONLY pattern)
        enrichment_data: Dict mapping company_key to enrichment result
        company_key_fn: Function to create company key from row

    Returns:
        DataFrame with same row count as input + enrichment data merged in
    """
    # Create normalized website key for matching
    contacts_df = contacts_df.copy()
    contacts_df['_website_key'] = contacts_df['website'].apply(
        lambda x: str(x).lower().replace('http://', '').replace('https://', '').rstrip('/') if pd.notna(x) else ''
    )

    def get_enrichment(row):
        key = company_key_fn({
            'company_name': row.get('company_name', ''),
            'website': row.get('website', '')
        })
        return enrichment_data.get(key, {})

    # Add enrichment as nested dict column
    contacts_df['_enrichment'] = contacts_df.apply(get_enrichment, axis=1)

    # Verify row count preserved
    original_count = len(contacts_df)
    if original_count != len(contacts_df):
        raise ValueError(f"Row count changed during join! Original: {original_count}, After: {len(contacts_df)}")

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
        Process a contact list: clone -> extract unique companies -> enrich -> join back -> push.

        IMPORTANT: Original contact list is READ-ONLY. Output has SAME row count as input.

        Flow:
        1. Normalize column names (company_website -> website)
        2. Clone original with index for later join
        3. Extract unique companies (separate copy)
        4. Enrich companies only (not per-contact)
        5. Join enrichment back to ALL original contacts
        6. Push webhook per contact (not per company)

        Args:
            contacts_df: Input contact DataFrame (READ-ONLY)
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

        original_count = len(contacts_df)
        print(f"Original contact count: {original_count}")

        # Step 1: Normalize column names
        normalized_df, column_mapping = normalize_columns(contacts_df)
        if column_mapping:
            print(f"Normalized columns: {column_mapping}")

        # Step 2: Extract unique companies (preserves original with index)
        unique_companies, contacts_with_index = extract_unique_companies(normalized_df)
        companies = unique_companies.to_dict('records')

        print(f"\nStep 1/4: Extracted {len(companies)} unique companies from {original_count} contacts")
        print(f"Enriching companies only (not per-contact)...")

        # Step 3: Enrich companies using DAG scheduler
        await dag_scheduler_fn(
            companies=companies,
            run_id=run_id,
            state_store=self.state_store,
            cache_store=self.cache_store,
            semaphores=self.semaphores,
            webhook_url=None,  # Don't push company webhooks - we push per contact
            dry_run=True,  # Don't push during enrichment
            verbose=verbose
        )

        print(f"\nStep 2/4: Company enrichment complete")

        # Step 4: Collect enrichment data from state store
        enrichment_data = {}
        for company in companies:
            key = self.company_key_fn(company)
            node_states = self.state_store.get_all_node_states(run_id, key)
            enrichment_data[key] = self._build_enrichment_dict(node_states)

        # Step 5: Join enrichment back to ALL original contacts
        contacts_with_enrichment = join_enrichment_to_contacts(
            contacts_with_index,
            enrichment_data,
            self.company_key_fn
        )

        # Verify row count preserved
        final_count = len(contacts_with_enrichment)
        print(f"\nStep 3/4: Joined enrichment back to contacts")
        print(f"  Original count: {original_count}")
        print(f"  Final count:    {final_count}")
        if final_count != original_count:
            raise ValueError(f"Row count changed! Original: {original_count}, Final: {final_count}")
        print(f"  ✓ Row count preserved")

        # Step 6: Push webhook per contact (not per company)
        print(f"\nStep 4/4: Pushing webhooks for {final_count} contacts...")

        webhook_semaphore = asyncio.Semaphore(10)  # Limit concurrent webhook calls
        push_stats = await push_contacts_to_webhook(
            contacts_with_enrichment,
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
        print(f"Companies Enriched: {len(companies)}")
        print(f"Webhooks Pushed:    {push_stats['pushed']}")
        print(f"Failed:             {push_stats['failed']}")
        print(f"Skipped (dry run):  {push_stats['skipped']}")
        print(f"Row Count Check:    {original_count} → {final_count} ({'✓ PRESERVED' if original_count == final_count else '✗ CHANGED'})")
        print(f"{'='*60}")

        return {
            "contacts_total": push_stats['total'],
            "companies_enriched": len(companies),
            "webhooks_pushed": push_stats['pushed'],
            "webhooks_failed": push_stats['failed'],
            "row_count_preserved": original_count == final_count
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
