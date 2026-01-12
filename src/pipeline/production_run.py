"""
Production Enrichment Pipeline Runner (DAG-based)

Runs enrichment on a CSV file using a DAG-based workflow with:
- Async execution with provider-specific rate limiting
- SQLite-backed state persistence for resumable runs
- TTL-based caching to avoid redundant API calls
- Streaming webhook pushes after each company completes

Usage:
    # Test with 50 company sample first
    python -m src.pipeline.production_run --file companies.csv --sample 50

    # Full production run (all companies)
    python -m src.pipeline.production_run --file companies.csv

    # Resume a failed run
    python -m src.pipeline.production_run --file companies.csv --resume prod_20240115_143022

    # Dry run (no webhook push)
    python -m src.pipeline.production_run --file companies.csv --sample 10 --dry-run
"""

import asyncio
import hashlib
import json
import os
import sys
import time
import requests
import pandas as pd
from asyncio import Semaphore
from pathlib import Path
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import new DAG modules
from src.pipeline.state_store import StateStore
from src.pipeline.cache_store import CacheStore
from src.pipeline.dag import NODES, get_ready_nodes, is_company_complete
from src.pipeline.nodes.mx_check import run_mx_check

# Import existing runners
from src.pipeline.runners import run_icp_qual, run_careers_linkedin, run_news_intel


# =============================================================================
# Config
# =============================================================================

CLAY_WEBHOOK = os.getenv("CLAY_WEBHOOK_URL", "")

# Provider concurrency limits
PROVIDER_LIMITS = {
    "openrouter": 50,
    "spider": 30,
    "serper": 40,
    "mx": 100,
    "webhook": 10,
}


# =============================================================================
# Provider Semaphores for Rate Limiting
# =============================================================================

class ProviderSemaphores:
    """Manage per-provider concurrency limits using asyncio semaphores."""

    def __init__(self):
        self.semaphores = {p: Semaphore(limit) for p, limit in PROVIDER_LIMITS.items()}

    async def acquire(self, provider: str):
        """Acquire semaphore for a provider. Blocks if at limit."""
        if provider in self.semaphores:
            await self.semaphores[provider].acquire()

    def release(self, provider: str):
        """Release semaphore for a provider."""
        if provider in self.semaphores:
            self.semaphores[provider].release()


# =============================================================================
# Utility Functions
# =============================================================================

def make_company_key(company: dict) -> str:
    """Create unique key from company_name + website."""
    s = f"{company.get('company_name', '')}|{company.get('website', '')}"
    return hashlib.md5(s.encode()).hexdigest()[:12]


# =============================================================================
# Node Runners
# =============================================================================

NODE_RUNNERS = {
    "mx_check": run_mx_check,
    "icp_check": run_icp_qual,
    "careers": run_careers_linkedin,
    "news": run_news_intel,
}


async def run_node_async(node_name: str, company: dict) -> dict:
    """Run a node asynchronously using thread pool for sync functions."""
    runner = NODE_RUNNERS.get(node_name)
    if not runner:
        return {"error": f"Unknown node: {node_name}"}

    name = company.get("company_name", "")
    website = company.get("website", "")

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, runner, name, website)


# =============================================================================
# StateStore Adapter
# =============================================================================

class StateStoreAdapter:
    """Adapts StateStore to match the Protocol expected by dag.py."""

    def __init__(self, store: StateStore, run_id: str):
        self.store = store
        self.run_id = run_id

    def get_node_status(self, company_key: str, node_name: str, run_id: str) -> Optional[str]:
        """Get status of a node."""
        state = self.store.get_node_state(run_id, company_key, node_name)
        return state["status"] if state else None

    def get_gate(self, company_key: str, gate_name: str, run_id: str) -> Optional[bool]:
        """Get gate value."""
        return self.store.get_gate(run_id, company_key, gate_name)

    def set_node_status(self, company_key: str, node_name: str, run_id: str, status: str) -> None:
        """Set status of a node."""
        self.store.update_node(run_id, company_key, node_name, status)


# =============================================================================
# Scoring Functions (same as before)
# =============================================================================

def score_icp_result(result: dict) -> dict:
    """Score ICP qualification result."""
    issues = []
    if "error" in result:
        return {"usable": False, "issues": [f"Error: {result['error']}"]}
    if result.get("is_b2b") is None:
        issues.append("is_b2b is null")
    if result.get("what_they_do") is None or result.get("what_they_do") == "":
        issues.append("what_they_do missing")
    if result.get("who_they_serve") is None or result.get("who_they_serve") == "":
        issues.append("who_they_serve missing")
    return {"usable": len(issues) == 0, "issues": issues}


def score_careers_result(result: dict) -> dict:
    """Score careers/LinkedIn result."""
    issues = []
    if "error" in result:
        return {"usable": False, "issues": [f"Error: {result['error']}"]}
    if result.get("is_hiring") is None:
        issues.append("is_hiring is null")
    if result.get("hiring_intensity") not in ["HOT", "WARM", "COLD"]:
        issues.append(f"Invalid hiring_intensity: {result.get('hiring_intensity')}")
    return {"usable": len(issues) == 0, "issues": issues}


def score_news_result(result: dict) -> dict:
    """Score news intelligence result."""
    issues = []
    if "error" in result:
        return {"usable": False, "issues": [f"Error: {result['error']}"]}
    if result.get("has_recent_news") is None:
        issues.append("has_recent_news is null")
    if result.get("outreach_timing") not in ["POSITIVE", "NEUTRAL", "NEGATIVE"]:
        issues.append(f"Invalid outreach_timing: {result.get('outreach_timing')}")
    return {"usable": len(issues) == 0, "issues": issues}


# =============================================================================
# Webhook Functions
# =============================================================================

def build_webhook_record(company_result: dict, run_id: str) -> dict:
    """Transform a single company result into a Clay-compatible record."""
    record = {
        "run_id": run_id,
        "company_name": company_result.get("company_name"),
        "website": company_result.get("website"),
        "processed_at": datetime.now().isoformat(),
        # ICP fields
        "ai_is_qualified": None,
        "ai_qualification_tier": "NEEDS_REVIEW",
        "ai_confidence": None,
        "ai_what_they_do": None,
        "ai_who_they_serve": None,
        "ai_three_sentence_summary": None,
        "ai_primary_industry": None,
        "ai_services_list": None,
        # Hiring fields
        "ai_hiring_intensity": None,
        "ai_is_hiring": None,
        "ai_open_positions_count": None,
        "ai_careers_url": None,
        "ai_linkedin_url": None,
        "ai_all_job_titles": None,
        # News fields
        "ai_has_recent_news": None,
        "ai_has_funding_news": None,
        "ai_outreach_timing": None,
        "ai_funding_amount": None,
        "ai_funding_round_type": None,
    }

    # Extract ICP data
    icp_data = company_result.get("icp", {})
    if icp_data and "result" in icp_data:
        icp = icp_data["result"]
        record["ai_is_qualified"] = icp.get("is_b2b")
        record["ai_confidence"] = icp.get("confidence")
        record["ai_what_they_do"] = icp.get("what_they_do")
        record["ai_who_they_serve"] = icp.get("who_they_serve")
        record["ai_three_sentence_summary"] = icp.get("three_sentence_summary")
        record["ai_primary_industry"] = icp.get("primary_industry")
        record["ai_services_list"] = json.dumps(icp.get("services_list", []))
        if icp.get("is_b2b") is True:
            record["ai_qualification_tier"] = "QUALIFIED"
        elif icp.get("is_b2b") is False:
            record["ai_qualification_tier"] = "DISQUALIFIED"

    # Extract Careers data
    careers_data = company_result.get("careers", {})
    if careers_data and "result" in careers_data:
        careers = careers_data["result"]
        record["ai_hiring_intensity"] = careers.get("hiring_intensity")
        record["ai_is_hiring"] = careers.get("is_hiring")
        record["ai_open_positions_count"] = careers.get("open_positions_count")
        record["ai_careers_url"] = careers.get("careers_url")
        record["ai_linkedin_url"] = careers.get("linkedin_url")
        record["ai_all_job_titles"] = json.dumps(careers.get("all_job_titles", []))

    # Extract News data
    news_data = company_result.get("news", {})
    if news_data and "result" in news_data:
        news = news_data["result"]
        record["ai_has_recent_news"] = news.get("has_recent_news")
        record["ai_has_funding_news"] = news.get("has_funding_news")
        record["ai_outreach_timing"] = news.get("outreach_timing")
        record["ai_funding_amount"] = news.get("funding_amount")
        record["ai_funding_round_type"] = news.get("funding_round_type")

    return record


def build_webhook_record_from_state(
    company: dict,
    state_store: StateStore,
    run_id: str,
    company_key: str
) -> dict:
    """Build webhook record from state store data."""
    record = {
        "run_id": run_id,
        "company_name": company.get("company_name"),
        "website": company.get("website"),
        "processed_at": datetime.now().isoformat(),
        # ICP fields
        "ai_is_qualified": None,
        "ai_qualification_tier": "NEEDS_REVIEW",
        "ai_confidence": None,
        "ai_what_they_do": None,
        "ai_who_they_serve": None,
        "ai_three_sentence_summary": None,
        "ai_primary_industry": None,
        "ai_services_list": None,
        # Hiring fields
        "ai_hiring_intensity": None,
        "ai_is_hiring": None,
        "ai_open_positions_count": None,
        "ai_careers_url": None,
        "ai_linkedin_url": None,
        "ai_all_job_titles": None,
        # News fields
        "ai_has_recent_news": None,
        "ai_has_funding_news": None,
        "ai_outreach_timing": None,
        "ai_funding_amount": None,
        "ai_funding_round_type": None,
    }

    # Get all node states
    node_states = state_store.get_all_node_states(run_id, company_key)

    # Extract ICP data
    icp_state = node_states.get("icp_check", {})
    if icp_state and icp_state.get("output"):
        icp = icp_state["output"]
        record["ai_is_qualified"] = icp.get("is_b2b")
        record["ai_confidence"] = icp.get("confidence")
        record["ai_what_they_do"] = icp.get("what_they_do")
        record["ai_who_they_serve"] = icp.get("who_they_serve")
        record["ai_three_sentence_summary"] = icp.get("three_sentence_summary")
        record["ai_primary_industry"] = icp.get("primary_industry")
        record["ai_services_list"] = json.dumps(icp.get("services_list", []))
        if icp.get("is_b2b") is True:
            record["ai_qualification_tier"] = "QUALIFIED"
        elif icp.get("is_b2b") is False:
            record["ai_qualification_tier"] = "DISQUALIFIED"

    # Extract Careers data
    careers_state = node_states.get("careers", {})
    if careers_state and careers_state.get("output"):
        careers = careers_state["output"]
        record["ai_hiring_intensity"] = careers.get("hiring_intensity")
        record["ai_is_hiring"] = careers.get("is_hiring")
        record["ai_open_positions_count"] = careers.get("open_positions_count")
        record["ai_careers_url"] = careers.get("careers_url")
        record["ai_linkedin_url"] = careers.get("linkedin_url")
        record["ai_all_job_titles"] = json.dumps(careers.get("all_job_titles", []))

    # Extract News data
    news_state = node_states.get("news", {})
    if news_state and news_state.get("output"):
        news = news_state["output"]
        record["ai_has_recent_news"] = news.get("has_recent_news")
        record["ai_has_funding_news"] = news.get("has_funding_news")
        record["ai_outreach_timing"] = news.get("outreach_timing")
        record["ai_funding_amount"] = news.get("funding_amount")
        record["ai_funding_round_type"] = news.get("funding_round_type")

    return record


def push_single_to_webhook(record: dict, webhook_url: str) -> dict:
    """Push a single company record to the webhook."""
    if not webhook_url:
        return {"error": "No webhook URL configured"}

    try:
        # Clay expects an array even for single records
        response = requests.post(
            webhook_url,
            json=[record],
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


async def push_webhook_async(
    company: dict,
    state_store: StateStore,
    run_id: str,
    company_key: str,
    webhook_url: str,
    semaphores: ProviderSemaphores
) -> dict:
    """Push webhook asynchronously with rate limiting."""
    await semaphores.acquire("webhook")
    try:
        record = build_webhook_record_from_state(company, state_store, run_id, company_key)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, push_single_to_webhook, record, webhook_url)
        if result.get("success"):
            state_store.mark_webhook_pushed(run_id, company_key)
        return result
    finally:
        semaphores.release("webhook")


# =============================================================================
# DAG Scheduler
# =============================================================================

async def run_dag_scheduler(
    companies: list[dict],
    run_id: str,
    state_store: StateStore,
    cache_store: CacheStore,
    semaphores: ProviderSemaphores,
    webhook_url: str = None,
    dry_run: bool = False,
    verbose: bool = False
) -> dict:
    """Run the DAG scheduler for all companies."""

    start_time = time.time()
    total = len(companies)

    # Progress counters
    progress = {
        "completed": 0,
        "mx_passed": 0,
        "mx_failed": 0,
        "icp_passed": 0,
        "icp_failed": 0,
        "webhook_pushed": 0,
        "webhook_failed": 0,
    }

    print(f"\n{'='*60}")
    print(f"DAG SCHEDULER - Processing {total} companies")
    print(f"{'='*60}")
    print(f"Run ID:     {run_id}")
    print(f"Webhook:    {'DRY RUN' if dry_run else (webhook_url[:50] + '...' if webhook_url else 'NOT CONFIGURED')}")
    print(f"{'='*60}\n")

    # Initialize companies in state store
    for company in companies:
        company_key = make_company_key(company)
        if not state_store.company_exists(run_id, company_key):
            state_store.init_company(run_id, company_key, company)

    # Create adapter for dag functions
    adapter = StateStoreAdapter(state_store, run_id)

    async def process_company(company: dict, index: int):
        """Process a single company through the DAG."""
        company_key = make_company_key(company)
        name = company.get("company_name", "Unknown")
        website = company.get("website", "")

        while True:
            ready_nodes = get_ready_nodes(company_key, adapter, run_id)

            if not ready_nodes:
                if is_company_complete(company_key, adapter, run_id):
                    state_store.set_company_status(run_id, company_key, "completed")
                    progress["completed"] += 1

                    # Push webhook
                    if webhook_url and not dry_run:
                        result = await push_webhook_async(
                            company, state_store, run_id, company_key, webhook_url, semaphores
                        )
                        if result.get("success"):
                            progress["webhook_pushed"] += 1
                        else:
                            progress["webhook_failed"] += 1
                            if verbose:
                                print(f"  [{name}] webhook: failed - {result.get('error')}")

                    # Progress update
                    elapsed = time.time() - start_time
                    rate = progress["completed"] / elapsed * 60 if elapsed > 0 else 0
                    eta = (total - progress["completed"]) / (rate / 60) if rate > 0 else 0

                    print(f"[{progress['completed']}/{total}] {name}: COMPLETE "
                          f"({rate:.1f}/min, ETA: {eta/60:.1f}min)")
                break

            # Execute ready nodes
            tasks = []
            for node_name in ready_nodes:
                node = NODES[node_name]

                # Skip webhook_push node here - handled separately
                if node_name == "webhook_push":
                    # Mark as completed since we handle it above
                    state_store.update_node(run_id, company_key, node_name, "completed")
                    continue

                # Check cache first
                cached = cache_store.get(node_name, website)
                if cached:
                    state_store.update_node(run_id, company_key, node_name, "completed", cached, cost=0)
                    # Set gates from cached result
                    if node_name == "mx_check":
                        mx_pass = cached.get("mx_valid", False)
                        state_store.set_gate(run_id, company_key, "mx_pass", mx_pass)
                        if mx_pass:
                            progress["mx_passed"] += 1
                        else:
                            progress["mx_failed"] += 1
                    elif node_name == "icp_check":
                        icp_pass = cached.get("is_b2b", False)
                        state_store.set_gate(run_id, company_key, "icp_pass", icp_pass)
                        if icp_pass:
                            progress["icp_passed"] += 1
                        else:
                            progress["icp_failed"] += 1
                    if verbose:
                        print(f"  [{name}] {node_name}: cached")
                    continue

                # Create async task for node execution
                tasks.append(execute_node(
                    node_name, node, company, company_key, name, website,
                    state_store, cache_store, semaphores, run_id, progress, verbose
                ))

            # Execute all ready nodes concurrently
            if tasks:
                await asyncio.gather(*tasks)

    async def execute_node(
        node_name, node, company, company_key, name, website,
        state_store, cache_store, semaphores, run_id, progress, verbose
    ):
        """Execute a single node with semaphore management."""
        await semaphores.acquire(node.provider)
        try:
            state_store.update_node(run_id, company_key, node_name, "running")
            result = await run_node_async(node_name, company)

            cost = result.get("cost_usd", 0)
            state_store.update_node(run_id, company_key, node_name, "completed", result, cost)

            # Cache the result
            cache_store.set(node_name, website, result, cost)

            # Set gates
            if node_name == "mx_check":
                mx_pass = result.get("mx_valid", False)
                state_store.set_gate(run_id, company_key, "mx_pass", mx_pass)
                if mx_pass:
                    progress["mx_passed"] += 1
                else:
                    progress["mx_failed"] += 1
            elif node_name == "icp_check":
                icp_pass = result.get("is_b2b", False)
                state_store.set_gate(run_id, company_key, "icp_pass", icp_pass)
                if icp_pass:
                    progress["icp_passed"] += 1
                else:
                    progress["icp_failed"] += 1

            if verbose:
                print(f"  [{name}] {node_name}: completed")

        except Exception as e:
            state_store.update_node(run_id, company_key, node_name, "failed", {"error": str(e)})
            if verbose:
                print(f"  [{name}] {node_name}: failed - {e}")
        finally:
            semaphores.release(node.provider)

    # Run all companies concurrently
    await asyncio.gather(*[process_company(c, i) for i, c in enumerate(companies)])

    # Get final stats
    elapsed = time.time() - start_time
    stats = state_store.get_run_stats(run_id)
    cache_stats = cache_store.get_stats()

    # Final report
    print(f"\n{'='*60}")
    print("DAG SCHEDULER COMPLETE")
    print(f"{'='*60}")
    print(f"Total Companies:    {total}")
    print(f"Completed:          {stats.get('status_counts', {}).get('completed', 0)}")
    print(f"MX Passed:          {stats['mx_passed']}")
    print(f"MX Failed:          {stats['mx_failed']}")
    print(f"ICP Passed:         {stats['icp_passed']}")
    print(f"ICP Failed:         {stats['icp_failed']}")
    print(f"Webhook Pushed:     {stats['webhooks_pushed']}")
    print(f"Total Cost:         ${stats['total_cost_usd']:.4f}")
    print(f"Cost per Company:   ${stats['total_cost_usd']/total:.4f}" if total > 0 else "N/A")
    print(f"Cache Hit Rate:     {cache_stats['hit_rate']:.1f}%")
    print(f"Time Elapsed:       {elapsed/60:.1f} minutes")
    print(f"Rate:               {total/elapsed*60:.1f} companies/min" if elapsed > 0 else "N/A")
    print(f"{'='*60}")

    return {
        "run_id": run_id,
        "stats": stats,
        "cache_stats": cache_stats,
        "elapsed_seconds": elapsed,
    }


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run DAG-based production enrichment pipeline")
    parser.add_argument("--file", "-f", required=True, help="CSV file with company_name,website columns")
    parser.add_argument("--sample", "-s", type=int, help="Only process first N companies (for testing)")
    parser.add_argument("--webhook", "-w", help="Override webhook URL from env")
    parser.add_argument("--dry-run", action="store_true", help="Skip webhook pushes")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--resume", "-r", help="Resume a previous run by run_id")
    parser.add_argument("--state-db", default=".state/pipeline.db", help="Path to state database")
    parser.add_argument("--cache-db", default=".state/cache.db", help="Path to cache database")
    parser.add_argument("--output", "-o", help="Save results to JSON file")

    args = parser.parse_args()

    # Load companies from CSV
    df = pd.read_csv(args.file)

    # Ensure required columns
    if "company_name" not in df.columns:
        # Try common alternatives
        if "Company Name" in df.columns:
            df = df.rename(columns={"Company Name": "company_name"})
        elif "name" in df.columns:
            df = df.rename(columns={"name": "company_name"})
        else:
            print("ERROR: CSV must have 'company_name' column")
            sys.exit(1)

    if "website" not in df.columns:
        if "Website" in df.columns:
            df = df.rename(columns={"Website": "website"})
        elif "url" in df.columns:
            df = df.rename(columns={"url": "website"})
        else:
            print("ERROR: CSV must have 'website' column")
            sys.exit(1)

    companies = df[["company_name", "website"]].to_dict("records")

    # Initialize stores
    state_store = StateStore(args.state_db)
    cache_store = CacheStore(args.cache_db)
    semaphores = ProviderSemaphores()

    # Determine run_id
    if args.resume:
        run_id = args.resume
        # Filter to resumable companies
        resumable = state_store.get_resumable_companies(run_id)
        resumable_keys = {c["company_key"] for c in resumable}

        # Filter input companies to only those that are resumable
        companies = [c for c in companies if make_company_key(c) in resumable_keys]
        print(f"Resuming run {run_id} with {len(companies)} resumable companies")
    else:
        run_id = f"prod_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Apply sample limit
    if args.sample:
        companies = companies[:args.sample]
        print(f"Sampling first {args.sample} companies")

    # Get webhook URL
    webhook_url = args.webhook or CLAY_WEBHOOK
    if not webhook_url and not args.dry_run:
        print("WARNING: No webhook URL configured. Use --webhook or set CLAY_WEBHOOK_URL env var.")
        print("         Running in dry-run mode.")
        args.dry_run = True

    # Run the DAG scheduler
    report = asyncio.run(run_dag_scheduler(
        companies=companies,
        run_id=run_id,
        state_store=state_store,
        cache_store=cache_store,
        semaphores=semaphores,
        webhook_url=webhook_url,
        dry_run=args.dry_run,
        verbose=args.verbose,
    ))

    # Save results
    if args.output:
        Path(args.output).write_text(
            json.dumps(report, indent=2, default=str),
            encoding="utf-8"
        )
        print(f"\nResults saved to: {args.output}")
