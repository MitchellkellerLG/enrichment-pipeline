"""
Spider Cloud API Configuration

Shared defaults for all Spider tools.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
SPIDER_API_KEY = os.getenv("SPIDER_API_KEY")
SPIDER_BASE_URL = "https://api.spider.cloud"

# Endpoints
ENDPOINTS = {
    "scrape": f"{SPIDER_BASE_URL}/scrape",
    "crawl": f"{SPIDER_BASE_URL}/crawl",
    "links": f"{SPIDER_BASE_URL}/links",
    "screenshot": f"{SPIDER_BASE_URL}/screenshot",
    "transform": f"{SPIDER_BASE_URL}/transform",
}

# Default parameters for all requests
# Note: wait_for removed - Spider default (500ms network idle) is sufficient for most pages
# Note: proxy disabled for cost savings - stealth + proxy doesn't affect base file_cost
DEFAULT_PARAMS = {
    "return_format": "markdown",
    "readability": True,
    "request": "smart",
    "stealth": False,           # Off for cost savings
    "cache": True,
    "max_credits_allowed": 20,  # Cap per run at $0.002
    "depth": 2,                 # Shallow crawl default
    # "proxy": "residential",   # Disabled - no proxy for cost savings
    "metadata": True,
    "block_ads": True,
    "block_analytics": True,
    # Performance optimizations - don't load unnecessary assets
    "block_images": True,
    "block_stylesheets": True,
    "clean_html": True,
}

# JS-heavy pages params - use Chrome for JavaScript rendering (e.g., careers pages)
# wait_for format: {"delay": {"secs": N, "nanos": 0}} or {"selector": {"timeout": {...}, "selector": "..."}}
JS_RENDERING_PARAMS = {
    "return_format": "markdown",
    "readability": True,
    "request": "chrome",         # Use headless Chrome for JS rendering
    "render_js": True,           # Enable JavaScript rendering
    "cache": True,
    "max_credits_allowed": 30,   # Higher limit for JS pages
    "depth": 2,
    "proxy": "residential",
    "metadata": True,
    "block_ads": True,
    "block_analytics": True,
    "filter_main_only": True,
    "wait_for": {"delay": {"secs": 3, "nanos": 0}},  # Wait 3s for JS to render
}

# Premium params - uses Bright Data Unblocker for protected sites
BRIGHTDATA_PROXY_URL = os.getenv(
    "BRIGHTDATA_UNBLOCKER_PROXY",
    "http://brd-customer-hl_0719f08c-zone-enrichment_agent:oti9h43ztjrn@brd.superproxy.io:33335"
)

PREMIUM_PARAMS = {
    "return_format": "markdown",
    "readability": True,
    "request": "chrome",         # Use headless Chrome for JS rendering
    "render_js": True,           # Enable JavaScript rendering
    "cache": False,              # Don't cache premium requests
    "max_credits_allowed": 50,   # Higher limit for protected pages
    "depth": 2,
    "proxy": "isp",              # Required when using remote_proxy
    "remote_proxy": BRIGHTDATA_PROXY_URL,  # Custom proxy (BYOP)
    "metadata": True,
    "block_ads": True,
    "block_analytics": True,
    "filter_main_only": True,
    "wait_for": {"delay": {"secs": 3, "nanos": 0}},  # Wait 3s for JS to render
    # Performance optimizations - don't load unnecessary assets
    "block_images": True,
    "block_stylesheets": True,
    "filter_images": True,
    "filter_output_images": True,
    "filter_svg": True,
    "filter_output_svg": True,
    "clean_html": True,
}

# Crawl-specific defaults
CRAWL_DEFAULTS = {
    "limit": 10,
    "depth": 2,
}

# Screenshot defaults
SCREENSHOT_DEFAULTS = {
    "full_page": True,
    "format": "png",
    "quality": 90,
}


def get_headers():
    """Get authorization headers for API requests."""
    if not SPIDER_API_KEY:
        raise ValueError("SPIDER_API_KEY not found in environment variables")

    return {
        "Authorization": f"Bearer {SPIDER_API_KEY}",
        "Content-Type": "application/json"
    }


def merge_params(custom_params: dict = None, base: dict = None) -> dict:
    """Merge custom parameters with defaults."""
    if base is None:
        base = DEFAULT_PARAMS.copy()
    else:
        base = base.copy()

    if custom_params:
        base.update(custom_params)

    return base


# =============================================================================
# Post-processing utilities
# =============================================================================

import re

def strip_markdown_images(content: str) -> str:
    """
    Remove markdown image syntax ![alt](url) from content.

    Use this in post-processing since filter_output_images only works
    with filter_main_only enabled, which can strip too much on some sites.
    """
    if not content:
        return content
    pattern = r'!\[[^\]]*\]\([^)]+\)'
    return re.sub(pattern, '', content)
