"""
Enrichment Pipeline v3 - Production Orchestrator

Full pipeline with conditional routing, retry logic, and webhook push.

Usage:
    python enrichment_pipeline_v3.py input.csv -o output.csv
    python enrichment_pipeline_v3.py input.csv --push-webhook --verbose
"""

import json
import sys
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add paths
sys.path.insert(0, str(Path(__file__).parent))
ENRICHMENT_AGENT_DIR = Path(__file__).parent.parent / ".claude/skills/enrichment/enrichment-agent/tools"
sys.path.insert(0, str(ENRICHMENT_AGENT_DIR))

from test_qualification_system import run_icp_qual, run_careers_linkedin, run_news_intel

# Config
CLAY_WEBHOOK = "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-967cf6cd-b14d-4721-9e4d-093a2b74e9a9"
MX_CHECK_API = "https://lg-mx-gateway-detector.up.railway.app/batch"


# =============================================================================
# MX Check
# =============================================================================

def check_mx_batch(domains: list) -> dict:
    """Check MX records for a batch of domains."""
    try:
        response = requests.post(
            MX_CHECK_API,
            json={"domains": domains},
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Webhook Push
# =============================================================================

def push_to_webhook(records: list, webhook_url: str = CLAY_WEBHOOK) -> dict:
    """Push enriched records to Clay webhook."""
    try:
        response = requests.post(
            webhook_url,
            json=records,
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        response.raise_for_status()
        return {"success": True, "count": len(records)}
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Pipeline Orchestrator
# =============================================================================

def run_enrichment_pipeline(
    companies: list,
    run_id: str = None,
    skip_mx_unsafe: bool = True,
    skip_disqualified: bool = True,
    push_webhook: bool = False,
    verbose: bool = False
) -> dict:
    """
    Run the full enrichment pipeline on a list of companies.

    Args:
        companies: List of {"company_name": ..., "website": ...}
        run_id: Unique run identifier
        skip_mx_unsafe: Skip hiring/news for MX unsafe domains
        skip_disqualified: Skip all further steps for disqualified companies
        push_webhook: Push results to Clay webhook
        verbose: Print progress

    Returns:
        dict with results, metrics, and run info
    """
    if run_id is None:
        run_id = f"enrichment_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    total = len(companies)
    results = []

    # Metrics
    qualified_count = 0
    disqualified_count = 0
    needs_review_count = 0
    mx_safe_count = 0
    mx_unsafe_count = 0
    hot_hiring_count = 0
    positive_timing_count = 0
    total_cost = 0

    print(f"\n{'='*60}")
    print(f"ENRICHMENT PIPELINE v3 - {run_id}")
    print(f"Total companies: {total}")
    print(f"{'='*60}")

    for i, company in enumerate(companies, 1):
        name = company.get("company_name", "Unknown")
        website = company.get("website", "")

        print(f"\n[{i}/{total}] {name}")

        record = {
            "run_id": run_id,
            "company_name": name,
            "website": website,
            # Index fields (stable)
            "ai_is_qualified": None,
            "ai_qualification_tier": None,
            "ai_confidence": None,
            "ai_fit_score": None,
            "ai_disqualify_reason": "N/A",
            "ai_mx_safe_to_cold_email": "N/A",
            "ai_hiring_intensity": "N/A",
            "ai_open_positions_count": "N/A",
            "ai_has_funding_news": "N/A",
            "ai_outreach_timing": "N/A",
            # Blobs
            "ai_icp": {},
            "ai_hiring": {},
            "ai_news": {},
            "ai_mx": {},
            # Meta
            "meta_cost_usd": 0,
            "meta_errors": []
        }

        # =================================================================
        # Stage 1: ICP Qualification (always first)
        # =================================================================
        print("  [1/4] ICP Qualification...")
        icp = run_icp_qual(name, website, use_retry=True, verbose=verbose)

        if "error" in icp:
            record["meta_errors"].append(f"ICP: {icp['error']}")
            record["ai_qualification_tier"] = "NEEDS_REVIEW"
            needs_review_count += 1
            print(f"    ERROR: {icp['error']}")
        else:
            is_b2b = icp.get("is_b2b")
            confidence = icp.get("confidence", 0)

            # Determine qualification tier
            if is_b2b is False:
                record["ai_is_qualified"] = False
                record["ai_qualification_tier"] = "DISQUALIFIED"
                record["ai_disqualify_reason"] = "Not B2B"
                disqualified_count += 1
                print(f"    DISQUALIFIED (B2C)")
            elif is_b2b is True and confidence >= 0.7:
                record["ai_is_qualified"] = True
                record["ai_qualification_tier"] = "QUALIFIED"
                qualified_count += 1
                print(f"    QUALIFIED (confidence: {confidence:.2f})")
            else:
                record["ai_is_qualified"] = True  # Allow through
                record["ai_qualification_tier"] = "NEEDS_REVIEW"
                needs_review_count += 1
                print(f"    NEEDS_REVIEW (confidence: {confidence:.2f})")

            record["ai_confidence"] = confidence
            record["ai_fit_score"] = int(confidence * 10)
            record["ai_icp"] = icp
            record["meta_cost_usd"] += icp.get("cost_usd", 0)

        # Skip further processing if disqualified
        if skip_disqualified and record["ai_qualification_tier"] == "DISQUALIFIED":
            results.append(record)
            total_cost += record["meta_cost_usd"]
            continue

        # =================================================================
        # Stage 2: MX Check (if qualified/needs_review)
        # =================================================================
        if website:
            print("  [2/4] MX Check...")
            domain = website.replace("http://", "").replace("https://", "").replace("www.", "").split("/")[0]

            mx_result = check_mx_batch([domain])
            if "error" in mx_result:
                record["meta_errors"].append(f"MX: {mx_result['error']}")
                record["ai_mx"] = {"error": mx_result["error"]}
                print(f"    ERROR: {mx_result['error']}")
            else:
                domain_result = mx_result.get("results", {}).get(domain, {})
                is_safe = domain_result.get("safe_to_cold_email", True)
                record["ai_mx_safe_to_cold_email"] = is_safe
                record["ai_mx"] = domain_result

                if is_safe:
                    mx_safe_count += 1
                    print(f"    MX SAFE")
                else:
                    mx_unsafe_count += 1
                    print(f"    MX UNSAFE ({domain_result.get('reason', 'unknown')})")
        else:
            print("  [2/4] MX Check... SKIPPED (no website)")

        # Skip hiring/news if MX unsafe
        if skip_mx_unsafe and record["ai_mx_safe_to_cold_email"] is False:
            results.append(record)
            total_cost += record["meta_cost_usd"]
            continue

        # =================================================================
        # Stage 3: Hiring/Careers (if MX safe)
        # =================================================================
        print("  [3/4] Hiring/Careers...")
        careers = run_careers_linkedin(name, website, use_retry=True, verbose=verbose)

        if "error" in careers:
            record["meta_errors"].append(f"Careers: {careers['error']}")
            print(f"    ERROR: {careers['error']}")
        else:
            record["ai_hiring_intensity"] = careers.get("hiring_intensity", "N/A")
            record["ai_open_positions_count"] = careers.get("open_positions_count", "N/A")
            record["ai_hiring"] = careers
            record["meta_cost_usd"] += careers.get("cost_usd", 0)

            if careers.get("hiring_intensity") == "HOT":
                hot_hiring_count += 1
            print(f"    {careers.get('hiring_intensity')} ({careers.get('open_positions_count', 0)} positions)")

        # =================================================================
        # Stage 4: News Intelligence (if MX safe)
        # =================================================================
        print("  [4/4] News Intelligence...")
        news = run_news_intel(name, website, use_retry=True, verbose=verbose)

        if "error" in news:
            record["meta_errors"].append(f"News: {news['error']}")
            print(f"    ERROR: {news['error']}")
        else:
            record["ai_has_funding_news"] = news.get("has_funding_news", False)
            record["ai_outreach_timing"] = news.get("outreach_timing", "N/A")
            record["ai_news"] = news
            record["meta_cost_usd"] += news.get("cost_usd", 0)

            if news.get("outreach_timing") == "POSITIVE":
                positive_timing_count += 1

            ice_breaker = news.get("ice_breaker", "")[:50]
            print(f"    {news.get('outreach_timing')} | {ice_breaker}...")

        results.append(record)
        total_cost += record["meta_cost_usd"]

    # =================================================================
    # Push to Webhook
    # =================================================================
    webhook_result = None
    if push_webhook:
        print(f"\n[WEBHOOK] Pushing {len(results)} records...")
        webhook_result = push_to_webhook(results)
        if "error" in webhook_result:
            print(f"  ERROR: {webhook_result['error']}")
        else:
            print(f"  SUCCESS: {webhook_result['count']} records pushed")

    # =================================================================
    # Summary
    # =================================================================
    print(f"\n{'='*60}")
    print("PIPELINE SUMMARY")
    print(f"{'='*60}")
    print(f"Total companies:     {total}")
    print(f"Qualified:           {qualified_count}")
    print(f"Disqualified:        {disqualified_count}")
    print(f"Needs Review:        {needs_review_count}")
    print(f"MX Safe:             {mx_safe_count}")
    print(f"MX Unsafe:           {mx_unsafe_count}")
    print(f"HOT Hiring:          {hot_hiring_count}")
    print(f"POSITIVE Timing:     {positive_timing_count}")
    print(f"Total Cost:          ${total_cost:.4f}")
    print(f"Cost per Company:    ${total_cost / total:.4f}")

    return {
        "run_id": run_id,
        "metrics": {
            "total_companies": total,
            "qualified": qualified_count,
            "disqualified": disqualified_count,
            "needs_review": needs_review_count,
            "mx_safe": mx_safe_count,
            "mx_unsafe": mx_unsafe_count,
            "hot_hiring": hot_hiring_count,
            "positive_timing": positive_timing_count,
            "total_cost": total_cost,
            "cost_per_company": total_cost / total
        },
        "results": results,
        "webhook_result": webhook_result,
        "timestamp": datetime.now().isoformat()
    }


def export_to_csv(results: list, output_path: str) -> str:
    """Export results to CSV with flattened index fields."""
    # Flatten records for CSV
    flat_records = []
    for r in results:
        flat = {
            "run_id": r.get("run_id"),
            "company_name": r.get("company_name"),
            "website": r.get("website"),
            "ai_is_qualified": r.get("ai_is_qualified"),
            "ai_qualification_tier": r.get("ai_qualification_tier"),
            "ai_confidence": r.get("ai_confidence"),
            "ai_fit_score": r.get("ai_fit_score"),
            "ai_disqualify_reason": r.get("ai_disqualify_reason"),
            "ai_mx_safe_to_cold_email": r.get("ai_mx_safe_to_cold_email"),
            "ai_hiring_intensity": r.get("ai_hiring_intensity"),
            "ai_open_positions_count": r.get("ai_open_positions_count"),
            "ai_has_funding_news": r.get("ai_has_funding_news"),
            "ai_outreach_timing": r.get("ai_outreach_timing"),
            "meta_cost_usd": r.get("meta_cost_usd"),
            "meta_errors": "; ".join(r.get("meta_errors", [])),
            # Include some key fields from blobs
            "icp_what_they_do": r.get("ai_icp", {}).get("what_they_do", ""),
            "icp_who_they_serve": r.get("ai_icp", {}).get("who_they_serve", ""),
            "hiring_careers_url": r.get("ai_hiring", {}).get("careers_url", ""),
            "hiring_linkedin_url": r.get("ai_hiring", {}).get("linkedin_url", ""),
            "news_ice_breaker": r.get("ai_news", {}).get("ice_breaker", ""),
        }
        flat_records.append(flat)

    df = pd.DataFrame(flat_records)
    df.to_csv(output_path, index=False)
    return output_path


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run enrichment pipeline v3")
    parser.add_argument("input", help="Input CSV file with company_name,website columns")
    parser.add_argument("--output", "-o", help="Output CSV file")
    parser.add_argument("--run-id", help="Custom run ID")
    parser.add_argument("--push-webhook", action="store_true", help="Push to Clay webhook")
    parser.add_argument("--no-skip-disqualified", action="store_true", help="Don't skip disqualified companies")
    parser.add_argument("--no-skip-mx-unsafe", action="store_true", help="Don't skip MX unsafe companies")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--max-rows", "-n", type=int, help="Limit number of rows to process")

    args = parser.parse_args()

    # Load input
    df = pd.read_csv(args.input)
    if args.max_rows:
        df = df.head(args.max_rows)
    companies = df.to_dict("records")

    # Run pipeline
    report = run_enrichment_pipeline(
        companies=companies,
        run_id=args.run_id,
        skip_mx_unsafe=not args.no_skip_mx_unsafe,
        skip_disqualified=not args.no_skip_disqualified,
        push_webhook=args.push_webhook,
        verbose=args.verbose
    )

    # Export
    if args.output:
        export_to_csv(report["results"], args.output)
        print(f"\nResults exported to: {args.output}")

    # Save full report
    report_path = f"pipeline_report_{report['run_id']}.json"
    Path(report_path).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"Full report saved to: {report_path}")
