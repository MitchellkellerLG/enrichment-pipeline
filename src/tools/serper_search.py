"""
Serper.dev Google Search Tool

Performs Google searches via Serper.dev API. Optimized for LLM consumption.

Usage:
    python serper_search.py "your search query"
    python serper_search.py "your search query" --news
"""

import os
import sys
import json
import argparse
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SERPER_ENDPOINT = "https://google.serper.dev"

# Locked-in defaults
DEFAULT_COUNTRY = "us"
DEFAULT_LANGUAGE = "en"
DEFAULT_NUM_RESULTS = 10


def search(query: str, news: bool = False, tbs: str = None) -> dict:
    """
    Perform a Google search via Serper.dev API.

    Args:
        query: Search query string
        news: If True, search news instead of web
        tbs: Time-based search filter. Examples:
             - "qdr:y" = past year
             - "qdr:m6" = past 6 months
             - "qdr:m3" = past 3 months
             - "qdr:m" = past month
             - "qdr:w" = past week
             - "qdr:d" = past day

    Returns:
        API response with search results (JSON)
    """
    if not SERPER_API_KEY:
        raise ValueError("SERPER_API_KEY not found in environment variables")

    search_type = "news" if news else "search"
    url = f"{SERPER_ENDPOINT}/{search_type}"

    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "q": query,
        "gl": DEFAULT_COUNTRY,
        "hl": DEFAULT_LANGUAGE,
        "num": DEFAULT_NUM_RESULTS
    }

    # Add time filter if specified
    if tbs:
        payload["tbs"] = tbs

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    return response.json()


def main():
    parser = argparse.ArgumentParser(
        description="Search Google via Serper.dev API (outputs JSON for LLM consumption)"
    )
    parser.add_argument(
        "query",
        help="Search query"
    )
    parser.add_argument(
        "--news",
        action="store_true",
        help="Search news instead of web results"
    )
    parser.add_argument(
        "--tbs",
        help="Time filter: qdr:y (year), qdr:m6 (6mo), qdr:m3 (3mo), qdr:m (month), qdr:w (week)"
    )

    args = parser.parse_args()

    try:
        results = search(query=args.query, news=args.news, tbs=args.tbs)
        print(json.dumps(results, indent=2))

    except requests.exceptions.HTTPError as e:
        error_response = {"error": str(e), "details": e.response.text}
        print(json.dumps(error_response, indent=2))
        sys.exit(1)
    except Exception as e:
        error_response = {"error": str(e)}
        print(json.dumps(error_response, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
