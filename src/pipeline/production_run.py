"""
Production Enrichment Pipeline Runner

Runs enrichment on a CSV file with streaming webhook pushes.
Each company's results are pushed to the webhook immediately after completion.

Usage:
    # Test with 50 company sample first
    python -m src.pipeline.production_run --file companies.csv --sample 50 --parallel 10

    # Full production run (all companies)
    python -m src.pipeline.production_run --file companies.csv --parallel 30

    # Dry run (no webhook push)
    python -m src.pipeline.production_run --file companies.csv --sample 10 --dry-run
"""

import json
import os
import sys
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from dotenv import load_dotenv
load_dotenv()

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.runners import run_icp_qual, run_careers_linkedin, run_news_intel

# =============================================================================
# Config
# =============================================================================

CLAY_WEBHOOK = os.getenv("CLAY_WEBHOOK_URL", "")
MAX_SAFE_PARALLEL = 30
STAGGER_INTERVAL = 0.1  # 100ms between each company start

# Thread-safe counters
stats_lock = Lock()
stats = {
    "total": 0,
    "completed": 0,
    "icp_success": 0,
    "careers_success": 0,
    "news_success": 0,
    "webhook_success": 0,
    "webhook_failed": 0,
    "total_cost": 0.0,
}


# =============================================================================
# Scoring (same as anneal_harness)
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
# Webhook Push (Single Company)
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


# =============================================================================
# Single Company Runner (with streaming webhook)
# =============================================================================

def run_single_company(
    company: dict,
    run_id: str,
    webhook_url: str = None,
    use_retry: bool = True,
    verbose: bool = False,
    dry_run: bool = False,
    stagger_delay: float = 0.0
) -> dict:
    """
    Run all 3 modules for a single company and push to webhook immediately.
    Thread-safe with stats tracking.
    """
    global stats

    name = company.get("company_name", "Unknown")
    website = company.get("website", "")

    # Stagger start to avoid thundering herd
    if stagger_delay > 0:
        time.sleep(stagger_delay)

    result = {
        "company_name": name,
        "website": website,
        "icp": None,
        "careers": None,
        "news": None
    }

    # Run ICP
    icp = run_icp_qual(name, website, use_retry=use_retry, verbose=verbose)
    icp_score = score_icp_result(icp)
    result["icp"] = {"result": icp, "score": icp_score}

    time.sleep(0.2)

    # Run Careers
    careers = run_careers_linkedin(name, website, use_retry=use_retry, verbose=verbose)
    careers_score = score_careers_result(careers)
    result["careers"] = {"result": careers, "score": careers_score}

    time.sleep(0.2)

    # Run News
    news = run_news_intel(name, website, use_retry=use_retry, verbose=verbose)
    news_score = score_news_result(news)
    result["news"] = {"result": news, "score": news_score}

    # Update stats
    with stats_lock:
        stats["completed"] += 1
        if icp_score["usable"]:
            stats["icp_success"] += 1
        if careers_score["usable"]:
            stats["careers_success"] += 1
        if news_score["usable"]:
            stats["news_success"] += 1
        stats["total_cost"] += icp.get("cost_usd", 0) + careers.get("cost_usd", 0) + news.get("cost_usd", 0)

    # Push to webhook immediately (streaming)
    if webhook_url and not dry_run:
        record = build_webhook_record(result, run_id)
        webhook_result = push_single_to_webhook(record, webhook_url)
        with stats_lock:
            if webhook_result.get("success"):
                stats["webhook_success"] += 1
            else:
                stats["webhook_failed"] += 1
                if verbose:
                    print(f"  [WEBHOOK ERROR] {name}: {webhook_result.get('error')}")

    return result


# =============================================================================
# Production Runner
# =============================================================================

def run_production(
    companies: list,
    webhook_url: str = None,
    parallel: int = 10,
    use_retry: bool = True,
    verbose: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Run enrichment on all companies with streaming webhook pushes.

    Args:
        companies: List of {"company_name": ..., "website": ...}
        webhook_url: Clay webhook URL (each company pushed individually)
        parallel: Number of concurrent workers
        use_retry: Use retry logic
        verbose: Print detailed output
        dry_run: Skip webhook pushes
    """
    global stats

    total = len(companies)
    run_id = f"prod_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Reset stats
    stats = {
        "total": total,
        "completed": 0,
        "icp_success": 0,
        "careers_success": 0,
        "news_success": 0,
        "webhook_success": 0,
        "webhook_failed": 0,
        "total_cost": 0.0,
    }

    print(f"\n{'='*60}")
    print(f"PRODUCTION RUN - {total} companies")
    print(f"{'='*60}")
    print(f"Run ID:     {run_id}")
    print(f"Parallel:   {parallel}")
    print(f"Webhook:    {'DRY RUN' if dry_run else (webhook_url[:50] + '...' if webhook_url else 'NOT CONFIGURED')}")
    print(f"{'='*60}\n")

    if parallel > MAX_SAFE_PARALLEL:
        print(f"WARNING: parallel={parallel} exceeds recommended max of {MAX_SAFE_PARALLEL}")

    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {}
        for i, company in enumerate(companies):
            stagger_delay = i * STAGGER_INTERVAL
            future = executor.submit(
                run_single_company,
                company,
                run_id,
                webhook_url,
                use_retry,
                verbose,
                dry_run,
                stagger_delay
            )
            futures[future] = company

        for i, future in enumerate(as_completed(futures), 1):
            company = futures[future]
            name = company.get("company_name", "Unknown")

            try:
                company_result = future.result()
                results.append(company_result)

                # Progress update
                icp_ok = "OK" if company_result["icp"]["score"]["usable"] else "FAIL"
                careers_ok = "OK" if company_result["careers"]["score"]["usable"] else "FAIL"
                news_ok = "OK" if company_result["news"]["score"]["usable"] else "FAIL"

                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta = (total - i) / rate if rate > 0 else 0

                print(f"[{i}/{total}] {name}: ICP:{icp_ok} | Careers:{careers_ok} | News:{news_ok}  ({rate:.1f}/min, ETA: {eta/60:.1f}min)")

            except Exception as e:
                print(f"[{i}/{total}] {name}: ERROR - {e}")

    # Final report
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print("PRODUCTION RUN COMPLETE")
    print(f"{'='*60}")
    print(f"Total Companies:    {stats['total']}")
    print(f"Completed:          {stats['completed']}")
    print(f"ICP Success:        {stats['icp_success']} ({stats['icp_success']/total*100:.1f}%)")
    print(f"Careers Success:    {stats['careers_success']} ({stats['careers_success']/total*100:.1f}%)")
    print(f"News Success:       {stats['news_success']} ({stats['news_success']/total*100:.1f}%)")
    print(f"Webhook Pushed:     {stats['webhook_success']}")
    print(f"Webhook Failed:     {stats['webhook_failed']}")
    print(f"Total Cost:         ${stats['total_cost']:.4f}")
    print(f"Cost per Company:   ${stats['total_cost']/total:.4f}")
    print(f"Time Elapsed:       {elapsed/60:.1f} minutes")
    print(f"Rate:               {total/elapsed*60:.1f} companies/min")
    print(f"{'='*60}")

    return {
        "run_id": run_id,
        "stats": stats.copy(),
        "results": results,
        "elapsed_seconds": elapsed,
    }


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run production enrichment pipeline")
    parser.add_argument("--file", "-f", required=True, help="CSV file with company_name,website columns")
    parser.add_argument("--sample", "-s", type=int, help="Only process first N companies (for testing)")
    parser.add_argument("--parallel", "-p", type=int, default=10, help=f"Concurrent workers (default: 10, max: {MAX_SAFE_PARALLEL})")
    parser.add_argument("--webhook", "-w", help="Override webhook URL from env")
    parser.add_argument("--dry-run", action="store_true", help="Skip webhook pushes")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--no-retry", action="store_true", help="Disable retry logic")
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

    # Run
    report = run_production(
        companies=companies,
        webhook_url=webhook_url,
        parallel=args.parallel,
        use_retry=not args.no_retry,
        verbose=args.verbose,
        dry_run=args.dry_run,
    )

    # Save results
    if args.output:
        Path(args.output).write_text(
            json.dumps(report, indent=2, default=str),
            encoding="utf-8"
        )
        print(f"\nResults saved to: {args.output}")
