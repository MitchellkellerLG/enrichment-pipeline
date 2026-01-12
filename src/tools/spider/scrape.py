"""
Spider Cloud Scraper - Single page content extraction.

Usage:
    python scrape.py https://example.com
    python scrape.py https://example.com --selector "article"
    python scrape.py https://example.com --no-readability --format text
    python scrape.py https://example.com -o output.md --json
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

from _config import ENDPOINTS, get_headers, merge_params, DEFAULT_PARAMS, JS_RENDERING_PARAMS


# URL patterns that typically need JavaScript rendering
JS_HEAVY_PATTERNS = [
    "/careers", "/jobs", "/openings", "/open-positions",
    "greenhouse.io", "lever.co", "workable.com", "ashbyhq.com",
    "boards.greenhouse", "jobs.lever", "apply.workable"
]


def needs_js_rendering(url: str) -> bool:
    """Detect if URL likely needs JavaScript rendering (e.g., careers pages)."""
    url_lower = url.lower()
    return any(pattern in url_lower for pattern in JS_HEAVY_PATTERNS)


def scrape_url(
    url: str,
    return_format: str = "markdown",
    readability: bool = True,
    root_selector: str = None,
    exclude_selector: str = None,
    cache: bool = True,
    metadata: bool = True,
    timeout: int = 120,  # 2 minutes - some pages are slow
    wait_for_js: bool = None,  # Auto-detect by default
) -> dict:
    """
    Scrape a single URL and return content.

    Args:
        url: URL to scrape
        return_format: 'markdown', 'text', 'raw'
        readability: Apply readability extraction
        root_selector: CSS selector for main content
        exclude_selector: CSS selector to exclude
        cache: Use HTTP caching
        metadata: Include title/description
        timeout: Request timeout in seconds
        wait_for_js: Force JS rendering mode (auto-detects if None)

    Returns:
        API response with content
    """
    # Auto-detect if JS rendering needed (e.g., careers pages)
    use_js = wait_for_js if wait_for_js is not None else needs_js_rendering(url)
    base_params = JS_RENDERING_PARAMS if use_js else DEFAULT_PARAMS

    payload = merge_params({
        "url": url,
        "return_format": return_format,
        "readability": readability,
        "cache": cache,
        "metadata": metadata,
    }, base=base_params)

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
        description="Scrape a URL using Spider Cloud API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape.py https://example.com
  python scrape.py https://example.com --selector "article"
  python scrape.py https://example.com --format text --no-readability
  python scrape.py https://example.com -o result.md
        """
    )
    parser.add_argument(
        "url",
        help="URL to scrape"
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
        "--no-cache",
        action="store_true",
        help="Bypass cache (fresh request)"
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Exclude title/description metadata"
    )
    parser.add_argument(
        "--wait-js",
        action="store_true",
        help="Force JavaScript rendering mode (auto-detects for careers pages)"
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

    # Check if JS rendering auto-detected
    js_mode = args.wait_js or needs_js_rendering(args.url)
    if js_mode:
        print(f"Scraping (JS mode): {args.url}")
    else:
        print(f"Scraping: {args.url}")

    try:
        result = scrape_url(
            url=args.url,
            return_format=args.format,
            readability=not args.no_readability,
            root_selector=args.selector,
            exclude_selector=args.exclude,
            cache=not args.no_cache,
            metadata=not args.no_metadata,
            timeout=args.timeout,
            wait_for_js=args.wait_js if args.wait_js else None,  # Let auto-detect work
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
                print(f"\n[Cost: {total:.5f} credits]")

    except requests.exceptions.HTTPError as e:
        print(f"API Error: {e}")
        print(f"Response: {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
