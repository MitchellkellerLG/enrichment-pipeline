"""
Spider Cloud Premium Scraper - Uses Bright Data Unblocker proxy for protected pages.

Bypasses anti-bot protection on sites like Crunchbase, LinkedIn, etc.
Uses Spider Cloud API with custom proxy routing through Bright Data.

Usage:
    python scrape_premium.py https://www.crunchbase.com/organization/byterover
    python scrape_premium.py https://example.com --selector "article"
    python scrape_premium.py https://example.com -o output.md --json
"""

import argparse
import io
import json
import sys
from pathlib import Path

import requests

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from _config import ENDPOINTS, get_headers, PREMIUM_PARAMS


def scrape_premium(
    url: str,
    return_format: str = "markdown",
    readability: bool = True,
    root_selector: str = None,
    exclude_selector: str = None,
    metadata: bool = True,
    timeout: int = 120,
) -> dict:
    """
    Scrape a protected URL using Spider Cloud API with Bright Data Unblocker proxy.

    Args:
        url: URL to scrape
        return_format: 'markdown', 'text', 'raw'
        readability: Apply readability extraction
        root_selector: CSS selector for main content
        exclude_selector: CSS selector to exclude
        metadata: Include title/description
        timeout: Request timeout in seconds (higher for premium)

    Returns:
        API response with content
    """
    # Start with premium params (includes Bright Data proxy)
    payload = PREMIUM_PARAMS.copy()

    # Override with function params
    payload.update({
        "url": url,
        "return_format": return_format,
        "readability": readability,
        "metadata": metadata,
    })

    if root_selector:
        payload["root_selector"] = root_selector
    if exclude_selector:
        payload["exclude_selector"] = exclude_selector

    response = requests.post(
        ENDPOINTS["scrape"],
        headers=get_headers(),
        json=payload,
        timeout=timeout
    )
    response.raise_for_status()

    return response.json()


def main():
    parser = argparse.ArgumentParser(
        description="Premium scraper using Spider Cloud + Bright Data Unblocker (bypasses anti-bot)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape_premium.py https://www.crunchbase.com/organization/byterover
  python scrape_premium.py https://linkedin.com/company/example --selector "main"
  python scrape_premium.py https://example.com -o result.md

Protected sites this works on:
  - Crunchbase
  - LinkedIn (public pages)
  - G2, Capterra
  - Other anti-bot protected sites
        """
    )
    parser.add_argument(
        "url",
        help="URL to scrape (works on protected sites)"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["markdown", "text", "raw"],
        default="markdown",
        help="Output format (default: markdown)"
    )
    parser.add_argument(
        "--selector", "-s",
        help="CSS selector for root content extraction"
    )
    parser.add_argument(
        "--exclude", "-e",
        help="CSS selector to exclude from content"
    )
    parser.add_argument(
        "--no-readability",
        action="store_true",
        help="Disable readability extraction"
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Exclude title/description metadata"
    )
    parser.add_argument(
        "--timeout", "-t",
        type=int,
        default=120,
        help="Request timeout in seconds (default: 120, higher for protected sites)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Save output to file"
    )
    parser.add_argument(
        "--json", "-j",
        action="store_true",
        help="Output raw JSON response"
    )

    args = parser.parse_args()

    print(f"Scraping (via Spider Cloud + Bright Data): {args.url}")

    try:
        result = scrape_premium(
            url=args.url,
            return_format=args.format,
            readability=not args.no_readability,
            root_selector=args.selector,
            exclude_selector=args.exclude,
            metadata=not args.no_metadata,
            timeout=args.timeout,
        )

        if args.json:
            output = json.dumps(result, indent=2)
        else:
            # Extract content from response
            if isinstance(result, list) and len(result) > 0:
                page = result[0]
                content = page.get("content", "")
                # Add metadata header if available
                if not args.no_metadata:
                    title = page.get("metadata", {}).get("title", "")
                    if title:
                        content = f"# {title}\n\n{content}"
            else:
                content = result.get("content", str(result))
            output = content

        if args.output:
            Path(args.output).write_text(output, encoding="utf-8")
            print(f"Saved to: {args.output}")
        else:
            print("\n" + "=" * 50)
            print(output)

        # Show cost info
        if isinstance(result, list) and len(result) > 0:
            costs = result[0].get("costs", {})
            if costs:
                total = costs.get("total_cost", 0)
                print(f"\n[Cost: {total:.5f} credits | Proxy: Bright Data Unblocker]")

    except requests.exceptions.HTTPError as e:
        print(f"API Error: {e}")
        print(f"Response: {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
