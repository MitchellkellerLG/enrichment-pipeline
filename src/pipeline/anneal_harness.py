"""
Anneal Harness for Enrichment Pipeline v3

Runs a sample batch of companies to measure accuracy before production.
Gate: Must reach >= 95% usable output accuracy to proceed.

Usage:
    python anneal_harness.py --sample 25
    python anneal_harness.py --file companies.csv --verbose
    python anneal_harness.py --parallel 3  # Run 3 companies concurrently
    python anneal_harness.py --push-webhook  # Push to Clay after gate passes
"""

import json
import sys
import time
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import os
from dotenv import load_dotenv
load_dotenv()

# Config
CLAY_WEBHOOK = os.getenv("CLAY_WEBHOOK_URL", "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-967cf6cd-b14d-4721-9e4d-093a2b74e9a9")

# Rate limiting - prevent API hammering
# OpenRouter: ~100 req/min per API key (we use 3 agents per company)
# Spider: ~100 req/min
# Serper: ~100 req/sec (generous)
# Safe limit: ~30 companies concurrently = 90 agents = ~90 req/min each
MAX_SAFE_PARALLEL = 30
API_SEMAPHORE = None  # Initialized at runtime

from src.pipeline.runners import run_icp_qual, run_careers_linkedin, run_news_intel


# =============================================================================
# Default Test Companies
# =============================================================================

DEFAULT_TEST_COMPANIES = [
    {"company_name": "Listen Labs", "website": "https://listenlabs.ai/"},
    {"company_name": "Traba", "website": "https://traba.work/"},
    {"company_name": "Vivodyne", "website": "https://www.vivodyne.com/"},
    {"company_name": "Gradient Labs", "website": "https://gradient-labs.ai/"},
    {"company_name": "OptimHire", "website": "https://optimhire.com/"},
    {"company_name": "Neysa", "website": "https://neysa.ai/"},
    {"company_name": "Div-IDy", "website": "https://www.div-idy.com/"},
    {"company_name": "Blossom Health", "website": "https://www.joinblossomhealth.com/"},
    {"company_name": "LLM Arena", "website": "https://lmarena.ai/"},
]


# =============================================================================
# Accuracy Scoring (v3.1 - Raw Data First)
# =============================================================================

def score_icp_result(result: dict) -> dict:
    """
    Score ICP qualification result.
    Returns {"usable": True/False, "issues": [...]}

    v3.1: Validates richer data extraction requirements.
    """
    issues = []

    if "error" in result:
        return {"usable": False, "issues": [f"Error: {result['error']}"]}

    # Check required fields
    if result.get("is_b2b") is None:
        issues.append("is_b2b is null")
    if result.get("what_they_do") is None or result.get("what_they_do") == "":
        issues.append("what_they_do missing")
    if result.get("who_they_serve") is None or result.get("who_they_serve") == "":
        issues.append("who_they_serve missing")

    # v3.1: Check new required fields
    if result.get("three_sentence_summary") is None or result.get("three_sentence_summary") == "":
        issues.append("three_sentence_summary missing")
    if result.get("primary_industry") is None or result.get("primary_industry") == "":
        issues.append("primary_industry missing")

    # v3.1: Check services_list has at least 1 item
    services = result.get("services_list", [])
    if not isinstance(services, list) or len(services) == 0:
        issues.append("services_list empty or missing")

    # Check evidence (required for all claims)
    evidence = result.get("evidence", [])
    if not isinstance(evidence, list) or len(evidence) == 0:
        issues.append("evidence array empty - all claims need evidence")
    elif isinstance(evidence, list):
        for e in evidence:
            if not e.get("url"):
                issues.append("Evidence missing URL")
                break

    return {"usable": len(issues) == 0, "issues": issues}


def score_careers_result(result: dict) -> dict:
    """
    Score careers/LinkedIn result.

    v3.1: Validates all_job_titles and audit trail.
    """
    issues = []

    if "error" in result:
        return {"usable": False, "issues": [f"Error: {result['error']}"]}

    if result.get("is_hiring") is None:
        issues.append("is_hiring is null")
    if result.get("hiring_intensity") not in ["HOT", "WARM", "COLD"]:
        issues.append(f"Invalid hiring_intensity: {result.get('hiring_intensity')}")

    # Check careers_url or linkedin_url exists
    if not result.get("careers_url") and not result.get("linkedin_url"):
        issues.append("Neither careers_url nor linkedin_url found")

    # v3.1: Check all_job_titles when hiring
    if result.get("is_hiring"):
        all_titles = result.get("all_job_titles", [])
        if not isinstance(all_titles, list) or len(all_titles) == 0:
            issues.append("is_hiring=True but all_job_titles empty")

    # v3.1: Check evidence
    evidence = result.get("evidence", [])
    if not isinstance(evidence, list) or len(evidence) == 0:
        issues.append("evidence array empty")

    return {"usable": len(issues) == 0, "issues": issues}


def score_news_result(result: dict) -> dict:
    """
    Score news intelligence result.

    v3.1: Validates recent_announcements, funding_details, and search_queries_used.
    """
    issues = []

    if "error" in result:
        return {"usable": False, "issues": [f"Error: {result['error']}"]}

    if result.get("has_recent_news") is None:
        issues.append("has_recent_news is null")
    if result.get("outreach_timing") not in ["POSITIVE", "NEUTRAL", "NEGATIVE"]:
        issues.append(f"Invalid outreach_timing: {result.get('outreach_timing')}")

    # v3.1: Check recent_announcements (renamed from news_items)
    announcements = result.get("recent_announcements", [])
    if not isinstance(announcements, list):
        issues.append("recent_announcements is not an array")
    else:
        # If has_funding_news is True, check for FUNDING item AND funding fields
        if result.get("has_funding_news"):
            funding_items = [n for n in announcements if n.get("type") == "FUNDING"]
            if len(funding_items) == 0:
                issues.append("has_funding_news=True but no FUNDING announcements")

            # v3.1: Check flattened funding fields when funding exists
            has_funding_detail = any([
                result.get("funding_round_type"),
                result.get("funding_amount"),
                result.get("funding_investors") and len(result.get("funding_investors", [])) > 0
            ])
            if not has_funding_detail:
                issues.append("has_funding_news=True but no funding details (round_type/amount/investors)")

    # v3.1: Check search_queries_used for auditability
    queries = result.get("search_queries_used", [])
    if not isinstance(queries, list) or len(queries) == 0:
        issues.append("search_queries_used empty or missing")

    return {"usable": len(issues) == 0, "issues": issues}


# =============================================================================
# Anneal Runner
# =============================================================================

def run_single_company(company: dict, use_retry: bool = True, verbose: bool = False, stagger_delay: float = 0.0) -> dict:
    """
    Run all 3 modules for a single company. Thread-safe.

    Args:
        stagger_delay: Initial delay before starting (used to stagger parallel starts)
    """
    global API_SEMAPHORE

    name = company.get("company_name", "Unknown")
    website = company.get("website", "")

    result = {
        "company_name": name,
        "website": website,
        "icp": None,
        "careers": None,
        "news": None
    }

    # Stagger start to avoid thundering herd
    if stagger_delay > 0:
        time.sleep(stagger_delay)

    # Run all 3 modules sequentially (they share API rate limits)
    # Small delays between modules prevent burst-hammering APIs
    icp = run_icp_qual(name, website, use_retry=use_retry, verbose=verbose)
    icp_score = score_icp_result(icp)
    result["icp"] = {"result": icp, "score": icp_score}

    time.sleep(0.2)  # Brief pause between modules

    careers = run_careers_linkedin(name, website, use_retry=use_retry, verbose=verbose)
    careers_score = score_careers_result(careers)
    result["careers"] = {"result": careers, "score": careers_score}

    time.sleep(0.2)

    news = run_news_intel(name, website, use_retry=use_retry, verbose=verbose)
    news_score = score_news_result(news)
    result["news"] = {"result": news, "score": news_score}

    return result


def run_anneal(
    companies: list = None,
    sample_size: int = 10,
    verbose: bool = False,
    use_retry: bool = True,
    parallel: int = 1
) -> dict:
    """
    Run anneal test on sample companies.

    Args:
        companies: List of {"company_name": ..., "website": ...}
        sample_size: Number of companies to test (if companies not provided)
        verbose: Print progress
        use_retry: Use retry logic in runners
        parallel: Number of companies to run concurrently (default 1 = sequential)

    Returns:
        dict with accuracy metrics and detailed results
    """
    if companies is None:
        companies = DEFAULT_TEST_COMPANIES[:sample_size]

    total = len(companies)
    results = []

    # Track metrics
    icp_usable = 0
    careers_usable = 0
    news_usable = 0
    total_cost = 0
    total_attempts = 0

    print(f"\n{'='*60}")
    print(f"ANNEAL TEST - {total} companies (parallel={parallel})")
    print(f"{'='*60}")

    # Warn if parallel is very high
    if parallel > MAX_SAFE_PARALLEL:
        print(f"⚠️  WARNING: parallel={parallel} exceeds recommended max of {MAX_SAFE_PARALLEL}")
        print(f"   This may cause rate limit errors. Consider using --parallel {MAX_SAFE_PARALLEL}")

    if parallel > 1:
        # PARALLEL EXECUTION with staggered starts
        # Stagger start times: 0.1s between each company to avoid thundering herd
        stagger_interval = 0.1  # 100ms between each start
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {}
            for i, company in enumerate(companies):
                stagger_delay = i * stagger_interval
                future = executor.submit(run_single_company, company, use_retry, verbose, stagger_delay)
                futures[future] = company

            for i, future in enumerate(as_completed(futures), 1):
                company = futures[future]
                name = company.get("company_name", "Unknown")

                try:
                    company_result = future.result()

                    # Score and tally
                    icp_score = company_result["icp"]["score"]
                    careers_score = company_result["careers"]["score"]
                    news_score = company_result["news"]["score"]

                    icp_data = company_result["icp"]["result"]
                    careers_data = company_result["careers"]["result"]
                    news_data = company_result["news"]["result"]

                    status = []
                    if icp_score["usable"]:
                        icp_usable += 1
                        status.append(f"ICP:OK")
                    else:
                        status.append(f"ICP:FAIL")

                    if careers_score["usable"]:
                        careers_usable += 1
                        status.append(f"Careers:OK")
                    else:
                        status.append(f"Careers:FAIL")

                    if news_score["usable"]:
                        news_usable += 1
                        status.append(f"News:OK")
                    else:
                        status.append(f"News:FAIL")

                    total_cost += icp_data.get("cost_usd", 0) + careers_data.get("cost_usd", 0) + news_data.get("cost_usd", 0)
                    total_attempts += icp_data.get("attempts", 1) + careers_data.get("attempts", 1) + news_data.get("attempts", 1)

                    print(f"[{i}/{total}] {name}: {' | '.join(status)}")
                    results.append(company_result)

                except Exception as e:
                    print(f"[{i}/{total}] {name}: ERROR - {e}")
    else:
        # SEQUENTIAL EXECUTION (original behavior)
        for i, company in enumerate(companies, 1):
            name = company.get("company_name", "Unknown")
            website = company.get("website", "")

            print(f"\n[{i}/{total}] {name}")
            print(f"  Website: {website}")

            company_result = {
                "company_name": name,
                "website": website,
                "icp": None,
                "careers": None,
                "news": None
            }

            # Run ICP
            print("  [ICP] Running...")
            icp = run_icp_qual(name, website, use_retry=use_retry, verbose=verbose)
            icp_score = score_icp_result(icp)
            company_result["icp"] = {"result": icp, "score": icp_score}
            if icp_score["usable"]:
                icp_usable += 1
                print(f"    OK (B2B: {icp.get('is_b2b')}, attempts: {icp.get('attempts', 1)})")
            else:
                print(f"    FAIL: {icp_score['issues']}")
            total_cost += icp.get("cost_usd", 0)
            total_attempts += icp.get("attempts", 1)

            # Run Careers
            print("  [CAREERS] Running...")
            careers = run_careers_linkedin(name, website, use_retry=use_retry, verbose=verbose)
            careers_score = score_careers_result(careers)
            company_result["careers"] = {"result": careers, "score": careers_score}
            if careers_score["usable"]:
                careers_usable += 1
                print(f"    OK (intensity: {careers.get('hiring_intensity')}, attempts: {careers.get('attempts', 1)})")
            else:
                print(f"    FAIL: {careers_score['issues']}")
            total_cost += careers.get("cost_usd", 0)
            total_attempts += careers.get("attempts", 1)

            # Run News
            print("  [NEWS] Running...")
            news = run_news_intel(name, website, use_retry=use_retry, verbose=verbose)
            news_score = score_news_result(news)
            company_result["news"] = {"result": news, "score": news_score}
            if news_score["usable"]:
                news_usable += 1
                print(f"    OK (timing: {news.get('outreach_timing')}, attempts: {news.get('attempts', 1)})")
            else:
                print(f"    FAIL: {news_score['issues']}")
            total_cost += news.get("cost_usd", 0)
            total_attempts += news.get("attempts", 1)

            results.append(company_result)

    # Calculate final metrics
    icp_rate = icp_usable / total * 100
    careers_rate = careers_usable / total * 100
    news_rate = news_usable / total * 100
    overall_rate = (icp_usable + careers_usable + news_usable) / (total * 3) * 100

    # Summary
    print(f"\n{'='*60}")
    print("ANNEAL RESULTS")
    print(f"{'='*60}")
    print(f"ICP Accuracy:      {icp_usable}/{total} ({icp_rate:.1f}%)")
    print(f"Careers Accuracy:  {careers_usable}/{total} ({careers_rate:.1f}%)")
    print(f"News Accuracy:     {news_usable}/{total} ({news_rate:.1f}%)")
    print(f"Overall Accuracy:  {icp_usable + careers_usable + news_usable}/{total * 3} ({overall_rate:.1f}%)")
    print(f"Total Cost:        ${total_cost:.4f}")
    print(f"Cost per Company:  ${total_cost / total:.4f}")
    print(f"Avg Attempts/Call: {total_attempts / (total * 3):.2f}")

    # Gate check
    gate_passed = overall_rate >= 95
    print(f"\n{'='*60}")
    if gate_passed:
        print("GATE: PASSED - Ready for production!")
    else:
        print(f"GATE: FAILED - Need {95 - overall_rate:.1f}% more accuracy")
    print(f"{'='*60}")

    return {
        "gate_passed": gate_passed,
        "metrics": {
            "icp_accuracy": icp_rate,
            "careers_accuracy": careers_rate,
            "news_accuracy": news_rate,
            "overall_accuracy": overall_rate,
            "total_cost": total_cost,
            "cost_per_company": total_cost / total,
            "avg_attempts_per_call": total_attempts / (total * 3)
        },
        "counts": {
            "total_companies": total,
            "icp_usable": icp_usable,
            "careers_usable": careers_usable,
            "news_usable": news_usable
        },
        "results": results,
        "timestamp": datetime.now().isoformat()
    }


def save_anneal_report(report: dict, output_path: str = None) -> str:
    """Save anneal report to JSON file."""
    if output_path is None:
        output_path = f"anneal_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    Path(output_path).write_text(
        json.dumps(report, indent=2, default=str),
        encoding="utf-8"
    )
    return output_path


def push_to_clay(report: dict, webhook_url: str = CLAY_WEBHOOK) -> dict:
    """
    Push anneal results to Clay webhook.
    Transforms the anneal results into Clay-compatible records.
    """
    records = []

    for company_result in report.get("results", []):
        # Build a flat record for Clay
        record = {
            "run_id": f"anneal_{report.get('timestamp', '')}",
            "company_name": company_result.get("company_name"),
            "website": company_result.get("website"),
            # ICP fields
            "ai_is_qualified": None,
            "ai_qualification_tier": "NEEDS_REVIEW",
            "ai_confidence": None,
            "ai_what_they_do": None,
            "ai_who_they_serve": None,
            # Hiring fields
            "ai_hiring_intensity": None,
            "ai_open_positions_count": None,
            "ai_careers_url": None,
            "ai_linkedin_url": None,
            # News fields
            "ai_has_funding_news": None,
            "ai_outreach_timing": None,
        }

        # Extract ICP data
        icp_data = company_result.get("icp", {})
        if icp_data and "result" in icp_data:
            icp = icp_data["result"]
            record["ai_is_qualified"] = icp.get("is_b2b")
            record["ai_confidence"] = icp.get("confidence")
            record["ai_what_they_do"] = icp.get("what_they_do")
            record["ai_who_they_serve"] = icp.get("who_they_serve")
            if icp.get("is_b2b") is True:
                record["ai_qualification_tier"] = "QUALIFIED"
            elif icp.get("is_b2b") is False:
                record["ai_qualification_tier"] = "DISQUALIFIED"

        # Extract Careers data
        careers_data = company_result.get("careers", {})
        if careers_data and "result" in careers_data:
            careers = careers_data["result"]
            record["ai_hiring_intensity"] = careers.get("hiring_intensity")
            record["ai_open_positions_count"] = careers.get("open_positions_count")
            record["ai_careers_url"] = careers.get("careers_url")
            record["ai_linkedin_url"] = careers.get("linkedin_url")

        # Extract News data
        news_data = company_result.get("news", {})
        if news_data and "result" in news_data:
            news = news_data["result"]
            record["ai_has_funding_news"] = news.get("has_funding_news")
            record["ai_outreach_timing"] = news.get("outreach_timing")

        records.append(record)

    # Push to webhook
    try:
        response = requests.post(
            webhook_url,
            json=records,
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        response.raise_for_status()
        return {"success": True, "count": len(records), "response": response.text[:200]}
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run anneal test for enrichment pipeline")
    parser.add_argument("--sample", "-s", type=int, default=9, help="Number of companies to test")
    parser.add_argument("--file", "-f", help="CSV file with company_name,website columns")
    parser.add_argument("--parallel", "-p", type=int, default=1, help=f"Run N companies concurrently (default: 1, recommended max: {MAX_SAFE_PARALLEL})")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--no-retry", action="store_true", help="Disable retry logic")
    parser.add_argument("--output", "-o", help="Output file for report")
    parser.add_argument("--push-webhook", action="store_true", help="Push results to Clay webhook")
    parser.add_argument("--force-push", action="store_true", help="Push to webhook even if gate fails")

    args = parser.parse_args()

    # Load companies
    companies = None
    if args.file:
        import pandas as pd
        df = pd.read_csv(args.file)
        companies = df.to_dict("records")

    # Run anneal
    report = run_anneal(
        companies=companies,
        sample_size=args.sample,
        verbose=args.verbose,
        use_retry=not args.no_retry,
        parallel=args.parallel
    )

    # Save report
    if args.output or report["gate_passed"]:
        path = save_anneal_report(report, args.output)
        print(f"\nReport saved to: {path}")

    # Push to Clay webhook
    if args.push_webhook:
        if report["gate_passed"] or args.force_push:
            print(f"\n[WEBHOOK] Pushing {len(report['results'])} records to Clay...")
            webhook_result = push_to_clay(report)
            if "error" in webhook_result:
                print(f"  ERROR: {webhook_result['error']}")
            else:
                print(f"  SUCCESS: {webhook_result['count']} records pushed")
        else:
            print(f"\n[WEBHOOK] Skipped - gate not passed. Use --force-push to override.")
