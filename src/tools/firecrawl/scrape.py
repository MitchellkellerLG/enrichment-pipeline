"""
Firecrawl Scraper - Anti-bot resistant web scraping.

Usage:
    python scrape.py https://example.com
    python scrape.py https://crunchbase.com/organization/acme
    python scrape.py https://example.com --format html
    python scrape.py https://example.com -o output.md --json

Firecrawl handles: proxies, caching, rate limits, JS-blocked content.
Works on protected sites like Crunchbase, ZoomInfo, etc.
"""

import argparse
import io
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
API_URL = "https://api.firecrawl.dev/v2/scrape"


def scrape_url(
    url: str,
    formats: list = None,
    only_main_content: bool = True,
    max_age: int = 172800000,  # 2 days default cache
    timeout: int = 60,
) -> dict:
    """
    Scrape a URL using Firecrawl API.

    Firecrawl handles anti-bot protection automatically.

    Args:
        url: URL to scrape
        formats: Output formats ['markdown', 'html', 'rawHtml', 'links', 'screenshot']
        only_main_content: Extract only main content (skip nav, footer)
        max_age: Cache max age in ms (0 = fresh, 172800000 = 2 days default)
        timeout: Request timeout in seconds

    Returns:
        API response with scraped content
    """
    if not FIRECRAWL_API_KEY:
        raise ValueError("FIRECRAWL_API_KEY not found in environment variables")

    if formats is None:
        formats = ["markdown"]

    headers = {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "url": url,
        "formats": formats,
        "onlyMainContent": only_main_content,
        "maxAge": max_age,
    }

    response = requests.post(
        API_URL,
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()

    return response.json()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape a URL using Firecrawl API (anti-bot resistant)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape.py https://example.com
  python scrape.py https://crunchbase.com/organization/acme
  python scrape.py https://example.com --format html --format markdown
  python scrape.py https://example.com --fresh -o result.md
        """
    )
    parser.add_argument(
        "url",
        help="URL to scrape"
    )
    parser.add_argument(
        "--format", "-f",
        action="append",
        choices=["markdown", "html", "rawHtml", "links", "screenshot"],
        help="Output format(s) - can specify multiple (default: markdown)"
    )
    parser.add_argument(
        "--full-page",
        action="store_true",
        help="Include full page (not just main content)"
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Bypass cache (force fresh scrape)"
    )
    parser.add_argument(
        "--timeout", "-t",
        type=int,
        default=60,
        help="Request timeout in seconds (default: 60)"
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

    formats = args.format if args.format else ["markdown"]

    print(f"Scraping: {args.url}")

    try:
        result = scrape_url(
            url=args.url,
            formats=formats,
            only_main_content=not args.full_page,
            max_age=0 if args.fresh else 172800000,
            timeout=args.timeout,
        )

        if not result.get("success", False):
            print(f"Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)

        data = result.get("data", {})

        if args.json:
            output = json.dumps(result, indent=2)
        else:
            # Prefer markdown, fall back to html
            content = data.get("markdown", data.get("html", ""))

            # Add metadata header
            metadata = data.get("metadata", {})
            title = metadata.get("title", "")
            if title:
                content = f"# {title}\n\n{content}"

            output = content

        if args.output:
            Path(args.output).write_text(output, encoding="utf-8")
            print(f"Saved to: {args.output}")
        else:
            print("\n" + "=" * 50)
            print(output[:3000] if len(output) > 3000 else output)
            if len(output) > 3000:
                print(f"\n... [truncated, {len(output)} total chars]")

        # Show metadata
        metadata = data.get("metadata", {})
        status = metadata.get("statusCode", "?")
        print(f"\n[Status: {status}]")

    except requests.exceptions.HTTPError as e:
        print(f"API Error: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text[:500]}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
