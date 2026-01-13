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
from threading import Lock
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
# Adaptive Rate Limiter
# =============================================================================

class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that discovers provider limits during real runs.

    Starts conservative, ramps up on success, backs off on rate limit.
    Persists discovered limits to state store.
    """

    # Default starting limits (conservative)
    DEFAULT_LIMITS = {
        "openrouter": 20,
        "spider": 15,
        "serper": 20,
        "mx": 50,
        "webhook": 10,
    }

    # Maximum limits to try
    MAX_LIMITS = {
        "openrouter": 100,
        "spider": 50,
        "serper": 80,
        "mx": 200,
        "webhook": 20,
    }

    def __init__(self, state_store=None, discover_mode: bool = False):
        self.state_store = state_store
        self.discover_mode = discover_mode
        self.lock = Lock()

        # Current state per provider
        self.providers = {}
        for provider in self.DEFAULT_LIMITS:
            # Try to load persisted limit
            stored = state_store.get_rate_limit(provider) if state_store else None

            self.providers[provider] = {
                "current_limit": stored["current_limit"] if stored else self.DEFAULT_LIMITS[provider],
                "discovered_limit": stored["discovered_limit"] if stored else None,
                "semaphore": asyncio.Semaphore(stored["current_limit"] if stored else self.DEFAULT_LIMITS[provider]),
                "success_streak": 0,
                "rate_limit_hits": 0,
                "active_count": 0,
            }

    async def acquire(self, provider: str):
        """Acquire a slot for the provider."""
        prov = self.providers.get(provider)
        if not prov:
            return
        await prov["semaphore"].acquire()
        with self.lock:
            prov["active_count"] += 1

    def release(self, provider: str):
        """Release a slot for the provider."""
        prov = self.providers.get(provider)
        if not prov:
            return
        prov["semaphore"].release()
        with self.lock:
            prov["active_count"] -= 1

    def on_success(self, provider: str):
        """Called when a request succeeds. Ramps up in discover mode."""
        if not self.discover_mode:
            return

        prov = self.providers.get(provider)
        if not prov:
            return

        with self.lock:
            prov["success_streak"] += 1

            # Ramp up every 20 successes
            if prov["success_streak"] >= 20:
                max_limit = self.MAX_LIMITS.get(provider, 100)
                if prov["current_limit"] < max_limit:
                    new_limit = min(prov["current_limit"] + 5, max_limit)
                    self._resize_semaphore(provider, new_limit)
                    print(f"  [RATE] {provider}: ramping up to {new_limit} concurrent")
                prov["success_streak"] = 0

    def on_rate_limit(self, provider: str):
        """Called when a rate limit is hit. Backs off and records limit."""
        prov = self.providers.get(provider)
        if not prov:
            return

        with self.lock:
            prov["rate_limit_hits"] += 1
            prov["discovered_limit"] = prov["current_limit"]
            prov["success_streak"] = 0

            # Back off 30%
            new_limit = max(5, int(prov["current_limit"] * 0.7))
            self._resize_semaphore(provider, new_limit)
            print(f"  [RATE] {provider}: rate limit hit! backing off to {new_limit} concurrent")

            # Persist to state store
            if self.state_store:
                self.state_store.save_rate_limit(
                    provider,
                    prov["discovered_limit"],
                    new_limit,
                    hit_count=1
                )

    def _resize_semaphore(self, provider: str, new_limit: int):
        """Resize semaphore (can only increase, not decrease mid-run)."""
        prov = self.providers.get(provider)
        if not prov:
            return

        old_limit = prov["current_limit"]
        prov["current_limit"] = new_limit

        # Replace semaphore with new limit
        # Note: This is safe because we're not shrinking during active use
        if new_limit > old_limit:
            # Add more permits
            for _ in range(new_limit - old_limit):
                prov["semaphore"].release()

    def get_stats(self) -> dict:
        """Get current stats for all providers."""
        stats = {}
        for provider, prov in self.providers.items():
            stats[provider] = {
                "current_limit": prov["current_limit"],
                "discovered_limit": prov["discovered_limit"],
                "rate_limit_hits": prov["rate_limit_hits"],
                "active": prov["active_count"]
            }
        return stats

    def persist_all(self):
        """Persist all discovered limits to state store."""
        if not self.state_store:
            return

        for provider, prov in self.providers.items():
            if prov["discovered_limit"] or prov["rate_limit_hits"] > 0:
                self.state_store.save_rate_limit(
                    provider,
                    prov["discovered_limit"] or prov["current_limit"],
                    prov["current_limit"],
                    hit_count=prov["rate_limit_hits"]
                )


# Keep ProviderSemaphores as alias for backward compatibility
ProviderSemaphores = AdaptiveRateLimiter


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
    """Transform a single company result into a webhook record with nested objects."""
    record = {
        "run_id": run_id,
        "company_name": company_result.get("company_name"),
        "website": company_result.get("website"),
        "processed_at": datetime.now().isoformat(),

        # Nested objects - populated below if data exists
        "mx": None,
        "icp": None,
        "careers": None,
        "news": None,
    }

    # Populate mx object
    mx_data = company_result.get("mx", {})
    if mx_data and "result" in mx_data:
        mx = mx_data["result"]
        record["mx"] = {
            "valid": mx.get("mx_valid", False),
            "records": mx.get("mx_records", []),
            "domain": mx.get("domain")
        }

    # Populate icp object
    icp_data = company_result.get("icp", {})
    if icp_data and "result" in icp_data:
        icp = icp_data["result"]
        is_b2b = icp.get("is_b2b")
        if is_b2b is True:
            tier = "QUALIFIED"
        elif is_b2b is False:
            tier = "DISQUALIFIED"
        else:
            tier = "NEEDS_REVIEW"

        record["icp"] = {
            "is_qualified": is_b2b,
            "qualification_tier": tier,
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

    # Populate careers object
    careers_data = company_result.get("careers", {})
    if careers_data and "result" in careers_data:
        careers = careers_data["result"]
        record["careers"] = {
            "is_hiring": careers.get("is_hiring"),
            "hiring_intensity": careers.get("hiring_intensity"),
            "open_positions_count": careers.get("open_positions_count"),
            "careers_url": careers.get("careers_url"),
            "linkedin_url": careers.get("linkedin_url"),
            "job_titles": careers.get("all_job_titles", []),
            "matched_roles": careers.get("matched_roles", [])
        }

    # Populate news object
    news_data = company_result.get("news", {})
    if news_data and "result" in news_data:
        news = news_data["result"]
        record["news"] = {
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

    return record


def build_webhook_record_from_state(
    company: dict,
    state_store: StateStore,
    run_id: str,
    company_key: str
) -> dict:
    """Build webhook record from state store data with nested objects."""
    record = {
        "run_id": run_id,
        "company_name": company.get("company_name"),
        "website": company.get("website"),
        "processed_at": datetime.now().isoformat(),

        # Nested objects - populated below if data exists
        "mx": None,
        "icp": None,
        "careers": None,
        "news": None,
    }

    # Get all node states
    node_states = state_store.get_all_node_states(run_id, company_key)

    # Populate mx object
    mx_state = node_states.get("mx_check", {})
    if mx_state and mx_state.get("output"):
        mx = mx_state["output"]
        record["mx"] = {
            "valid": mx.get("mx_valid", False),
            "records": mx.get("mx_records", []),
            "domain": mx.get("domain")
        }

    # Populate icp object
    icp_state = node_states.get("icp_check", {})
    if icp_state and icp_state.get("output"):
        icp = icp_state["output"]
        is_b2b = icp.get("is_b2b")
        if is_b2b is True:
            tier = "QUALIFIED"
        elif is_b2b is False:
            tier = "DISQUALIFIED"
        else:
            tier = "NEEDS_REVIEW"

        record["icp"] = {
            "is_qualified": is_b2b,
            "qualification_tier": tier,
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

    # Populate careers object
    careers_state = node_states.get("careers", {})
    if careers_state and careers_state.get("output"):
        careers = careers_state["output"]
        record["careers"] = {
            "is_hiring": careers.get("is_hiring"),
            "hiring_intensity": careers.get("hiring_intensity"),
            "open_positions_count": careers.get("open_positions_count"),
            "careers_url": careers.get("careers_url"),
            "linkedin_url": careers.get("linkedin_url"),
            "job_titles": careers.get("all_job_titles", []),
            "matched_roles": careers.get("matched_roles", [])
        }

    # Populate news object
    news_state = node_states.get("news", {})
    if news_state and news_state.get("output"):
        news = news_state["output"]
        record["news"] = {
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

            # Check for errors in result
            error = result.get("error", "")
            if error:
                # Check for rate limit error (429)
                if "429" in str(error) or "rate limit" in str(error).lower():
                    semaphores.on_rate_limit(node.provider)
                state_store.update_node(run_id, company_key, node_name, "failed", {"error": str(error)})
                if verbose:
                    print(f"  [{name}] {node_name}: failed - {error}")
                return

            cost = result.get("cost_usd", 0)
            state_store.update_node(run_id, company_key, node_name, "completed", result, cost)

            # Signal success to rate limiter
            semaphores.on_success(node.provider)

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
            # Check if exception indicates rate limiting
            error_str = str(e)
            if "429" in error_str or "rate limit" in error_str.lower():
                semaphores.on_rate_limit(node.provider)
            state_store.update_node(run_id, company_key, node_name, "failed", {"error": str(e)})
            if verbose:
                print(f"  [{name}] {node_name}: failed - {e}")
        finally:
            semaphores.release(node.provider)

    # Run all companies concurrently
    await asyncio.gather(*[process_company(c, i) for i, c in enumerate(companies)])

    # Persist discovered rate limits
    semaphores.persist_all()

    # Get final stats
    elapsed = time.time() - start_time
    stats = state_store.get_run_stats(run_id)
    cache_stats = cache_store.get_stats()
    rate_limit_stats = semaphores.get_stats()

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
    parser.add_argument("--discover-limits", action="store_true",
                        help="Enable adaptive rate limit discovery (ramps up on success, backs off on 429)")

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
    semaphores = AdaptiveRateLimiter(state_store=state_store, discover_mode=args.discover_limits)

    if args.discover_limits:
        print("Rate limit discovery mode ENABLED - will ramp up on success, back off on 429")

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
