"""
Enrichment Pipeline Runners

Runs the three enrichment agents:
1. ICP Qualification - Determine if company is B2B, what they do, who they serve
2. Careers & LinkedIn - Find hiring status, open roles, LinkedIn profile
3. News Intelligence - Find funding, partnerships, recent announcements

Key principles:
- Variable inputs at BOTTOM of prompts (prompt caching)
- Flag AI-generated columns
- Retry with model fallback on parse failure
"""

import json
import os
import re
import sys
import requests
import pandas as pd
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Config
CLAY_WEBHOOK = os.getenv("CLAY_WEBHOOK_URL", "https://api.clay.com/v3/sources/webhook/...")
MX_CHECK_API = "https://lg-mx-gateway-detector.up.railway.app/batch"


# =============================================================================
# JSON SALVAGE UTILITIES (Layer 1 - deterministic, no LLM cost)
# =============================================================================

def extract_json_object(text: str) -> str | None:
    """Extract first JSON object from text, stripping markdown fences and code wrappers."""
    if not text:
        return None
    text = text.strip()
    # Strip Python code wrappers (model quirk)
    text = re.sub(r"<tool_code>.*?data\s*=\s*", "", text, flags=re.DOTALL)
    text = re.sub(r"print\(.*?\)\s*'''.*$", "", text, flags=re.DOTALL)

    # Look for JSON block inside markdown fence first (more reliable)
    fence_match = re.search(r"```(?:json)?\s*(\{[^`]*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fence_match:
        return fence_match.group(1)

    # Fallback: find first {...} by brace matching
    start = text.find('{')
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape = False
    for i, c in enumerate(text[start:], start):
        if escape:
            escape = False
            continue
        if c == '\\' and in_string:
            escape = True
            continue
        if c == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                return text[start:i+1]

    # Fallback to simple greedy match (may fail on nested)
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    return match.group(0) if match else None


def safe_json_loads(text: str) -> dict | None:
    """Safely parse JSON from LLM output, handling markdown fences."""
    raw = extract_json_object(text)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None



def build_website_finder_prompt(
    company_name: str,
    industry: str = None,
    location: str = None,
    list_icp_context: str = None
) -> str:
    """
    Build website finder prompt with variable inputs at BOTTOM for caching.
    """
    # Put company name directly in the instruction for clarity
    prompt = f"""Find the website for: {company_name}

Call web_search with query: "{company_name} website"

From results, return the company's website (not LinkedIn/Crunchbase).

Return JSON: {{"found": true, "website": "https://...", "confidence": 0.9, "match_type": "exact_domain"}}"""

    return prompt


def find_website(
    company_name: str,
    industry: str = None,
    location: str = None,
    list_icp_context: str = None,
    verbose: bool = False
) -> dict:
    """
    Find a company's website using the enrichment agent.
    """
    from src.tools.call_llm_openrouter import run_agent

    prompt = build_website_finder_prompt(
        company_name=company_name,
        industry=industry,
        location=location,
        list_icp_context=list_icp_context
    )

    result = run_agent(
        prompt=prompt,
        tools=["web_search"],  # Only need search, not scraping
        max_turns=3,
        verbose=verbose
    )

    if "error" in result:
        return {
            "found": False,
            "error": result["error"],
            "ai_generated": True
        }

    # Parse JSON from response
    content = result.get("content", "")
    try:
        import re
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group(0))
            data["ai_generated"] = True
            data["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
            return data
    except:
        pass

    return {
        "found": False,
        "error": "Could not parse response",
        "raw_response": content[:500],
        "ai_generated": True
    }


def build_qualification_prompt(company_name: str, website: str, description: str = None) -> str:
    """
    Build qualification prompt with variable inputs at BOTTOM for caching.

    Static instructions at top (cached), variable data at bottom.
    """
    # STATIC PART (cached across calls)
    static_part = """# B2B Qualification Check

<task>
Determine if this company is a B2B business that would be a good prospect for a B2B service provider.
</task>

<available_tools>
You have access to ONLY these tools:
- web_search: Search the web for information (use query parameter)
- scrape_url: Extract content from a URL (use url parameter)

DO NOT attempt to use any other tools. These are the only valid tools.
</available_tools>

<context>
You are qualifying companies for outreach. A qualified company:
- Sells primarily to businesses (B2B), not consumers (B2C)
- Has a clear service/product offering
- Is not a personal blog, portfolio, or placeholder site
</context>

<instructions>
1. If a description is provided, use it as primary context - you may not need to scrape
2. If a website is provided and you need more info, use scrape_url to get the homepage content
3. Look for B2B indicators:
   - Enterprise/business pricing
   - Team/company features
   - Case studies with business logos
   - API documentation
   - Industry-specific B2B language
4. Look for disqualifying signals:
   - Consumer-focused language
   - Individual/personal pricing only
   - App store links without enterprise option
   - Portfolio/personal site

IMPORTANT: After analyzing, you MUST return a JSON object with your qualification decision.
</instructions>

<output_format>
Return ONLY this JSON format (no other text):
{
  "is_qualified": true/false,
  "is_b2b": true/false,
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation",
  "business_type": "Agency|SaaS|Consultancy|Marketplace|Other",
  "disqualify_reason": "Only if is_qualified=false"
}
</output_format>

---
# VARIABLE INPUTS (below this line changes per company)
"""

    # VARIABLE PART (changes per company, at bottom)
    variable_part = f"""
Company Name: {company_name}
Website: {website}
"""

    if description and str(description).strip() and str(description) != 'nan':
        variable_part += f"""
Existing Description (from CSV):
{description}

NOTE: Use this description as primary context. Only scrape website if you need to verify or supplement this information.
"""
    else:
        variable_part += """
No description available - please scrape the website to qualify.
"""

    return static_part + variable_part


def qualify_company(company_name: str, website: str, description: str = None, verbose: bool = False) -> dict:
    """
    Qualify a single company using the enrichment agent.
    """
    from src.tools.call_llm_openrouter import run_agent

    prompt = build_qualification_prompt(company_name, website, description)

    # Determine if we need to scrape
    has_description = description and str(description).strip() and str(description) != 'nan'
    tools = "websearch" if has_description else "all"  # Only scrape if no description

    result = run_agent(
        prompt=prompt,
        tools=["scrape_url", "web_search"] if not has_description else ["web_search"],
        max_turns=3,
        verbose=verbose
    )

    if "error" in result:
        return {
            "is_qualified": False,
            "error": result["error"],
            "ai_generated": True
        }

    # Parse JSON from response
    content = result.get("content", "")
    try:
        import re
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group(0))
            data["ai_generated"] = True
            data["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
            return data
    except:
        pass

    return {
        "is_qualified": False,
        "error": "Could not parse response",
        "raw_response": content[:500],
        "ai_generated": True
    }


# =============================================================================
# NEW QUALIFICATION PROMPTS
# =============================================================================

def build_icp_qual_prompt(company_name: str, website: str, description: str = None) -> str:
    """
    Build LeadGrow ICP qualification prompt.
    B2B check + rich factual data extraction.
    Strict JSON output - no markdown, no commentary.

    v3.1: Raw Data First - extract facts, not opinions.
    """
    static_part = """# LeadGrow ICP Qualification

<available_tools>
You have access to ONLY these tools:
- web_search: Search the web for information (use query parameter)
- scrape_url: Extract content from a URL (use url parameter)
DO NOT attempt to use any other tools.
</available_tools>

<task>
Qualify this company for B2B outreach. Extract rich factual data from their website.
</task>

<instructions>
1. Scrape the homepage to understand what the company does
2. Look for additional pages: /solutions, /products, /customers, /case-studies, /about
3. Determine if they are B2B (sells to businesses) or B2C (sells to consumers):
   - B2B indicators: enterprise pricing, team features, API docs, case studies, "Request Demo"
   - B2C indicators: individual pricing, consumer language, app store links

EXTRACTION REQUIREMENTS (Raw Facts Only):
- three_sentence_summary: Sentence 1 = what they do. Sentence 2 = who they serve. Sentence 3 = their primary value proposition.
- primary_industry: Be SPECIFIC (e.g., "Staffing & Workforce Technology" not just "Technology")
- services_list: Extract the EXACT services/products listed on the website, not a summary
- industries_served: List ALL industries mentioned, not just the primary one
- buyer_personas: Extract from "Who we serve", "Solutions for", or similar pages
- case_studies: Find case studies/success stories - return title + URL + first sentence

CRITICAL: Every non-null fact MUST have evidence. If you cannot find evidence, set the field to null.
</instructions>

<output_format>
Return ONLY valid JSON. No markdown code fences. No text before or after.
Use null for missing values.

{
  "is_b2b": true,
  "confidence": 0.85,
  "three_sentence_summary": "Sentence 1: what they do. Sentence 2: who they serve. Sentence 3: their value proposition.",
  "primary_industry": "Specific industry name (e.g., 'Staffing & Workforce Technology')",
  "what_they_do": "Their core product/service in detail",
  "who_they_serve": "Their target customer profile with specifics",
  "services_list": ["Service 1 as listed on site", "Service 2 as listed on site", "Service 3"],
  "industries_served": ["Industry 1", "Industry 2", "Industry 3"],
  "buyer_personas": ["VP Operations", "HR Director", "Warehouse Manager"],
  "problems_they_solve": ["Problem 1", "Problem 2"],
  "case_studies": [
    {
      "title": "Case study title as shown on site",
      "url": "https://company.com/customers/case-study",
      "snippet": "First sentence or summary from the case study"
    }
  ],
  "notable_customers": ["Customer 1", "Customer 2"],
  "evidence": [
    {"claim": "Provides on-demand staffing", "url": "https://company.com/", "snippet": "Exact quote from page"},
    {"claim": "Serves logistics companies", "url": "https://company.com/industries", "snippet": "Exact quote"}
  ],
  "disqualify_reason": null
}

CRITICAL: Your response must be ONLY the JSON object. Nothing else.
</output_format>

---
"""

    variable_part = f"""Company Name: {company_name}
Website: {website}
"""
    if description and str(description).strip() and str(description) != 'nan':
        variable_part += f"Description: {description}\n"

    return static_part + variable_part


def build_careers_linkedin_prompt(company_name: str, website: str) -> str:
    """
    Build careers page + LinkedIn finder prompt.
    Key insight: Search "{company_name} jobs" gets both in one search.
    Strict JSON output - no markdown, no commentary.

    v3.1: Raw Data First - exact job titles, audit trail for matched roles.
    """
    # Define the keywords we use for matching - included in prompt for transparency
    role_keywords = '["Account Executive", "Sales", "Revenue", "Marketing", "Growth", "Head of", "Director", "VP", "SDR", "BDR", "Customer Success"]'

    return f"""# Careers & LinkedIn Finder

<available_tools>
You have access to ONLY these tools:
- web_search: Search the web for information (use query parameter)
- scrape_url: Extract content from a URL (use url parameter)
DO NOT attempt to use any other tools.
</available_tools>

<task>
Find careers page and LinkedIn profile. Extract ALL job titles for audit trail.
</task>

<instructions>
1. Search: "{company_name} jobs" - this returns both careers page AND LinkedIn
2. Identify careers URL and LinkedIn company URL from results
3. If careers page found, scrape it and extract EVERY job title exactly as written
4. If scrape_url fails, use firecrawl_scrape (anti-bot resistant) on the careers page
5. Match job titles against these keywords: {role_keywords}
6. Classify hiring intensity:
   - HOT: Hiring sales/marketing/leadership roles (matches keywords above)
   - WARM: Hiring but only technical/operational roles (no keyword matches)
   - COLD: No open positions

EXTRACTION REQUIREMENTS (Raw Facts Only):
- all_job_titles: Return EVERY job title exactly as written on the careers page
- matched_roles: For each matched role, show which keyword matched (audit trail)
- keywords_used: Return the exact keyword list used for matching

IMPORTANT: If scraping fails but you found job titles in search results, use those.
IMPORTANT: Always output JSON even if you only have partial data - use empty arrays [] for missing data.

CONSTRAINTS (learned from failures):
- If is_hiring=True, all_job_titles MUST have at least one title (extract from search results if scrape fails)
- If you set hiring_intensity to HOT or WARM, you MUST have extracted job titles
- After 5 tool calls, you MUST output JSON with whatever data you have collected
- Never return null for hiring_intensity - use COLD if no data found

CRITICAL: Every claim must have evidence. Include the careers page URL as evidence.
</instructions>

<output_format>
Return ONLY valid JSON. No markdown code fences. No text before or after.
Use null for missing values.

{{{{
  "careers_url": "https://company.com/careers",
  "linkedin_url": "https://linkedin.com/company/companyname",
  "is_hiring": true,
  "hiring_intensity": "HOT",
  "open_positions_count": 12,
  "all_job_titles": [
    "Account Executive - Enterprise",
    "Senior Account Executive - Mid-Market",
    "Software Engineer - Backend",
    "Product Manager",
    "Head of Revenue Operations"
  ],
  "matched_roles": [
    {{{{"title": "Account Executive - Enterprise", "matched_keyword": "Account Executive"}}}},
    {{{{"title": "Head of Revenue Operations", "matched_keyword": "Revenue"}}}}
  ],
  "keywords_used": {role_keywords},
  "evidence": [
    {{{{"claim": "12 open positions", "url": "https://company.com/careers", "snippet": "View all 12 open positions"}}}}
  ]
}}}}

CRITICAL: Your response must be ONLY the JSON object. Nothing else.
</output_format>

---
Company Name: {company_name}
Website: {website}
"""


def build_news_intel_prompt(company_name: str, website: str) -> str:
    """
    Build news intelligence prompt.
    Finds recent news (last 12 months) and classifies relevance for B2B outreach.
    Strict JSON output - hardened schema where empty fields are valid results.

    v3.1: Raw Data First - capture ALL major announcements, structured funding details.
    """
    return f"""# News Intelligence

<available_tools>
You have access to ONLY these tools:
- web_search: Search the web for information (use query parameter)
DO NOT attempt to use any other tools. Do NOT scrape news articles.
</available_tools>

<task>
Find recent company announcements (last 12 months). Extract factual details from news.
</task>

<instructions>
TWO SEARCHES REQUIRED:
1. Search: "{company_name} funding raised series" - look for funding/investment news
2. Search: "{company_name} partnership launch announcement" - look for other major news

WHAT TO CAPTURE (All announcement types matter):
- FUNDING: Funding rounds, investments
- PARTNERSHIP: Strategic partnerships, integrations
- PRODUCT: New product launches, feature announcements
- EXPANSION: Geographic expansion, new markets
- ACQUISITION: M&A activity (acquirer or target)
- LEADERSHIP: Executive hires, leadership changes
- AWARD: Industry recognition, awards
- OTHER: Any other major company announcement

FOR FUNDING NEWS - Extract these details from the article:
- round_type: Series A, B, C, Seed, etc.
- amount: Dollar amount raised
- date: When announced
- investors: Named investors (lead + participants)
- use_of_proceeds: What they plan to do with the money (this IS stated in articles - extract it)

FOR EACH ANNOUNCEMENT:
- three_sentence_summary: Factual summary extracted from the article (not your interpretation)

Record the exact search queries you used for auditability.

TIMING CLASSIFICATION:
- POSITIVE: Funding, expansion, partnerships = good timing
- NEUTRAL: No news OR awards/product launches
- NEGATIVE: Layoffs, acquisition (as target) = bad timing
</instructions>

<output_format>
IMPORTANT: Your ENTIRE response must be ONLY the JSON object below.
Do NOT explain your reasoning. Do NOT describe the search results.

If no news found:
{{{{
  "has_recent_news": false,
  "has_funding_news": false,
  "funding_round_type": null,
  "funding_amount": null,
  "funding_date": null,
  "funding_investors": [],
  "funding_use_of_proceeds": null,
  "funding_source_url": null,
  "recent_announcements": [],
  "outreach_timing": "NEUTRAL",
  "search_queries_used": ["{company_name} funding raised series", "{company_name} partnership launch announcement"]
}}}}

If news IS found:
{{{{
  "has_recent_news": true,
  "has_funding_news": true,
  "funding_round_type": "Series B",
  "funding_amount": "$100M",
  "funding_date": "2024-06-15",
  "funding_investors": ["Khosla Ventures", "General Catalyst"],
  "funding_use_of_proceeds": "Expand into new markets, grow technology team",
  "funding_source_url": "https://techcrunch.com/...",
  "recent_announcements": [
    {{{{
      "headline": "Company Raises $100M Series B",
      "date": "2024-06-15",
      "type": "FUNDING",
      "source_url": "https://techcrunch.com/...",
      "three_sentence_summary": "Factual sentence 1 from article. Factual sentence 2. Factual sentence 3."
    }}}},
    {{{{
      "headline": "Company Partners with Amazon",
      "date": "2024-08-20",
      "type": "PARTNERSHIP",
      "source_url": "https://businesswire.com/...",
      "three_sentence_summary": "Factual summary of the partnership announcement."
    }}}}
  ],
  "outreach_timing": "POSITIVE",
  "search_queries_used": ["{company_name} funding raised series", "{company_name} partnership launch announcement"]
}}}}

STOP: Output ONLY the JSON object. Start with {{{{ and end with }}}}.
</output_format>

---
Company Name: {company_name}
Website: {website}
"""


def run_icp_qual(company_name: str, website: str, description: str = None, verbose: bool = False, use_retry: bool = True) -> dict:
    """
    Run ICP qualification prompt.

    Args:
        use_retry: If True, use run_agent_with_retry for automatic retries on failure
    """
    from src.tools.call_llm_openrouter import run_agent, run_agent_with_retry
    from src.schemas.definitions import ICP_REQUIRED_KEYS

    prompt = build_icp_qual_prompt(company_name, website, description)

    # Required keys for validation (v3.1 - richer data)
    required_keys = ICP_REQUIRED_KEYS

    # Note: Don't use response_format with tools - OpenRouter doesn't support mixing
    # tool calls with structured output enforcement. Rely on retry/validation instead.
    if use_retry:
        result = run_agent_with_retry(
            prompt=prompt,
            tools=["scrape_url", "web_search"],
            max_turns=5,  # More turns to allow scraping multiple pages
            verbose=verbose,
            required_keys=required_keys,
            max_retries=2
        )

        if "error" in result:
            return {"error": result["error"], "attempts": result.get("attempts", 0)}

        # run_agent_with_retry already parsed and validated
        data = result.get("parsed", {})
        data["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
        data["attempts"] = result.get("attempts", 1)
        return data
    else:
        # Original behavior without retry
        result = run_agent(
            prompt=prompt,
            tools=["scrape_url", "web_search"],
            max_turns=3,
            verbose=verbose
        )

        if "error" in result:
            return {"error": result["error"]}

        content = result.get("content", "")
        data = safe_json_loads(content)
        if data:
            data["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
            return data

        return {"error": "Could not parse response", "raw": content[:500]}


def run_careers_linkedin(company_name: str, website: str, verbose: bool = False, use_retry: bool = True) -> dict:
    """
    Run careers/LinkedIn finder prompt.

    Args:
        use_retry: If True, use run_agent_with_retry for automatic retries on failure
    """
    from src.tools.call_llm_openrouter import run_agent, run_agent_with_retry
    from src.schemas.definitions import CAREERS_REQUIRED_KEYS

    prompt = build_careers_linkedin_prompt(company_name, website)

    # Required keys for validation (v3.1 - richer data with audit trail)
    required_keys = CAREERS_REQUIRED_KEYS

    # Note: Don't use response_format with tools - OpenRouter doesn't support mixing
    # tool calls with structured output enforcement. Rely on retry/validation instead.
    if use_retry:
        result = run_agent_with_retry(
            prompt=prompt,
            tools=["scrape_url", "web_search", "firecrawl_scrape"],  # Include firecrawl for blocked pages
            max_turns=7,  # More turns - careers pages often need multiple scrapes
            verbose=verbose,
            required_keys=required_keys,
            max_retries=2
        )

        if "error" in result:
            return {"error": result["error"], "attempts": result.get("attempts", 0)}

        data = result.get("parsed", {})
        data["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
        data["attempts"] = result.get("attempts", 1)
        return data
    else:
        result = run_agent(
            prompt=prompt,
            tools=["scrape_url", "web_search", "firecrawl_scrape"],
            max_turns=5,
            verbose=verbose
        )

        if "error" in result:
            return {"error": result["error"]}

        content = result.get("content", "")
        data = safe_json_loads(content)
        if data:
            data["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
            return data

    return {"error": "Could not parse response", "raw": content[:500]}


def run_news_intel(company_name: str, website: str, verbose: bool = False, use_retry: bool = True) -> dict:
    """
    Run news intelligence prompt.

    Args:
        use_retry: If True, use run_agent_with_retry for automatic retries on failure
    """
    from src.tools.call_llm_openrouter import run_agent, run_agent_with_retry
    from src.schemas.definitions import NEWS_REQUIRED_KEYS

    prompt = build_news_intel_prompt(company_name, website)

    # Required keys for validation (v3.1 - richer data with funding details)
    required_keys = NEWS_REQUIRED_KEYS

    # Note: Don't use response_format with tools - OpenRouter doesn't support mixing
    # tool calls with structured output enforcement. Rely on retry/validation instead.
    if use_retry:
        result = run_agent_with_retry(
            prompt=prompt,
            tools=["web_search"],
            max_turns=5,  # More turns to allow multiple searches
            verbose=verbose,
            required_keys=required_keys,
            max_retries=2
        )

        if "error" in result:
            return {"error": result["error"], "attempts": result.get("attempts", 0)}

        data = result.get("parsed", {})
        data["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
        data["attempts"] = result.get("attempts", 1)
        return data
    else:
        result = run_agent(
            prompt=prompt,
            tools=["web_search"],
            max_turns=3,
            verbose=verbose
        )

        if "error" in result:
            return {"error": result["error"]}

        content = result.get("content", "")
        data = safe_json_loads(content)
        if data:
            data["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
            return data

        return {"error": "Could not parse response", "raw": content[:500]}


def run_gtm_enrichment_test(companies: list, verbose: bool = False):
    """
    Test all 3 GTM prompts on a list of companies.

    Args:
        companies: List of dicts with 'company_name' and 'website' keys
        verbose: Print detailed output
    """
    print(f"\n{'='*60}")
    print("GTM ENRICHMENT TEST - 3 PROMPTS")
    print(f"{'='*60}")

    results = []

    for i, company in enumerate(companies):
        company_name = company.get("company_name", "Unknown")
        website = company.get("website", "")
        description = company.get("description", "")

        print(f"\n[{i+1}/{len(companies)}] {company_name}")
        print(f"  Website: {website}")

        result = {
            "company_name": company_name,
            "website": website
        }

        # 1. ICP Qualification
        print("  [ICP] Running ICP Qualification...")
        icp = run_icp_qual(company_name, website, description, verbose)
        if "error" not in icp:
            result["icp_is_b2b"] = icp.get("is_b2b", False)
            result["icp_summary"] = icp.get("three_sentence_summary", "")
            result["icp_what_they_do"] = icp.get("what_they_do", "")
            result["icp_who_they_serve"] = icp.get("who_they_serve", "")
            problems = icp.get("problems_they_solve", [])
            result["icp_problems"] = ", ".join(problems) if isinstance(problems, list) else str(problems or "")
            result["icp_industry"] = icp.get("industry_focus", "")
            customers = icp.get("notable_customers", [])
            result["icp_customers"] = ", ".join(customers) if isinstance(customers, list) else str(customers or "")
            result["icp_cost"] = icp.get("cost_usd", 0)
            print(f"    B2B: {icp.get('is_b2b')} | Industry: {icp.get('industry_focus')}")
        else:
            result["icp_error"] = icp.get("error")
            print(f"    ERROR: {icp.get('error')}")

        # 2. Careers + LinkedIn
        print("  [CAREERS] Running Careers/LinkedIn Finder...")
        careers = run_careers_linkedin(company_name, website, verbose)
        if "error" not in careers:
            result["careers_url"] = careers.get("careers_url", "")
            result["linkedin_url"] = careers.get("linkedin_url", "")
            result["is_hiring"] = careers.get("is_hiring", False)
            result["hiring_intensity"] = careers.get("hiring_intensity", "")
            result["hiring_sales_marketing"] = careers.get("hiring_sales_marketing", False)
            result["hiring_leadership"] = careers.get("hiring_leadership", False)
            result["open_positions"] = careers.get("open_positions_count", 0)
            roles = careers.get("notable_roles", [])
            result["notable_roles"] = ", ".join(roles) if isinstance(roles, list) else str(roles or "")
            result["growth_signals"] = careers.get("growth_signals", "")
            result["careers_cost"] = careers.get("cost_usd", 0)
            print(f"    Hiring: {careers.get('hiring_intensity')} | LinkedIn: {'Y' if careers.get('linkedin_url') else 'N'}")
        else:
            result["careers_error"] = careers.get("error")
            print(f"    ERROR: {careers.get('error')}")

        # 3. News Intelligence
        print("  [NEWS] Running News Intelligence...")
        news = run_news_intel(company_name, website, verbose)
        if "error" not in news:
            result["has_recent_news"] = news.get("has_recent_news", False)
            result["outreach_timing"] = news.get("outreach_timing", "")
            result["timing_reason"] = news.get("timing_reason", "")
            result["ice_breaker"] = news.get("ice_breaker", "")
            result["key_insight"] = news.get("key_insight", "")
            result["news_cost"] = news.get("cost_usd", 0)
            ice_breaker = news.get('ice_breaker') or ''
            print(f"    Timing: {news.get('outreach_timing')} | Ice Breaker: {ice_breaker[:50]}...")
        else:
            result["news_error"] = news.get("error")
            print(f"    ERROR: {news.get('error')}")

        # Calculate total cost
        total_cost = result.get("icp_cost", 0) + result.get("careers_cost", 0) + result.get("news_cost", 0)
        result["total_cost"] = total_cost
        print(f"  Total cost: ${total_cost:.4f}")

        results.append(result)

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    total_companies = len(results)
    b2b_count = sum(1 for r in results if r.get("icp_is_b2b"))
    hiring_hot = sum(1 for r in results if r.get("hiring_intensity") == "HOT")
    positive_timing = sum(1 for r in results if r.get("outreach_timing") == "POSITIVE")
    total_cost = sum(r.get("total_cost", 0) for r in results)

    print(f"Total companies: {total_companies}")
    print(f"B2B: {b2b_count}/{total_companies}")
    print(f"HOT hiring: {hiring_hot}/{total_companies}")
    print(f"POSITIVE timing: {positive_timing}/{total_companies}")
    print(f"Total cost: ${total_cost:.4f}")
    print(f"Cost per company: ${total_cost/total_companies:.4f}")

    # Save results
    output_path = Path(__file__).parent / "gtm_enrichment_results.csv"
    pd.DataFrame(results).to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path}")

    return results


def check_mx_batch(domains: list) -> dict:
    """
    Check MX records for a batch of domains using Railway API.
    Returns dict keyed by domain.
    """
    try:
        response = requests.post(
            MX_CHECK_API,
            headers={"Content-Type": "application/json"},
            json={"domains": domains},
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            # Convert results array to dict keyed by domain
            results_by_domain = {}
            for result in data.get("results", []):
                domain = result.get("domain")
                if domain:
                    results_by_domain[domain] = {
                        "has_gateway": result.get("has_security_gateway", False),
                        "gateway_name": result.get("security_gateway", {}).get("name", "") if result.get("security_gateway") else "",
                        "gateway_severity": result.get("security_gateway", {}).get("severity", "") if result.get("security_gateway") else "",
                        "email_provider": result.get("email_provider", ""),
                        "safe_to_cold_email": result.get("safe_to_cold_email", True)
                    }
            return results_by_domain
        else:
            return {"error": f"API returned {response.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def push_to_clay(records: list, webhook_url: str) -> dict:
    """
    Push records to Clay webhook.
    """
    import math

    # Clean records - replace NaN/inf with None for JSON compatibility
    clean_records = []
    for record in records:
        clean_record = {}
        for key, value in record.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                clean_record[key] = None
            elif value == "nan" or str(value) == "nan":
                clean_record[key] = None
            else:
                clean_record[key] = value
        clean_records.append(clean_record)

    try:
        response = requests.post(
            webhook_url,
            headers={"Content-Type": "application/json"},
            json={"records": clean_records},
            timeout=30
        )

        return {
            "status_code": response.status_code,
            "response": response.text[:500] if response.text else "No response"
        }
    except Exception as e:
        return {"error": str(e)}


def run_qualification_test(input_csv: str, max_rows: int = 10, verbose: bool = False):
    """
    Run the full qualification test.
    """
    print(f"\n{'='*60}")
    print("QUALIFICATION SYSTEM TEST")
    print(f"{'='*60}")

    # Load data
    print(f"\n[1/4] Loading data from: {input_csv}")
    df = pd.read_csv(input_csv, nrows=max_rows)
    print(f"  Loaded {len(df)} companies")

    # Identify columns (prefer exact matches)
    website_col = None
    for col in df.columns:
        # Prefer exact "Website" match
        if col.lower() == 'website':
            website_col = col
            break
    if not website_col:
        for col in df.columns:
            if col.lower() in ['website', 'company website', 'domain']:
                website_col = col
                break

    company_col = None
    for col in df.columns:
        if col.lower() == 'company name':
            company_col = col
            break
    if not company_col:
        for col in df.columns:
            if 'company' in col.lower() and 'name' in col.lower():
                company_col = col
                break

    desc_col = None
    for col in df.columns:
        if 'company short description' in col.lower():
            desc_col = col
            break
    if not desc_col:
        for col in df.columns:
            if 'description' in col.lower():
                desc_col = col
                break

    # Also detect industry and location columns for website finder
    industry_col = None
    for col in df.columns:
        if col.lower() in ['industry', 'company industry']:
            industry_col = col
            break

    location_col = None
    for col in df.columns:
        if col.lower() in ['city', 'location', 'state']:
            location_col = col
            break

    print(f"  Website column: {website_col}")
    print(f"  Company column: {company_col}")
    print(f"  Description column: {desc_col}")
    print(f"  Industry column: {industry_col}")
    print(f"  Location column: {location_col}")

    # Step 1.5: Find missing websites
    print(f"\n[1.5/5] Finding missing websites...")
    websites_found = 0
    for idx, row in df.iterrows():
        website = row.get(website_col, "") if website_col else ""
        if not website or str(website).strip() == "" or str(website) == "nan":
            company_name = row.get(company_col, f"Company {idx}")
            industry = row.get(industry_col, "") if industry_col else ""
            location = row.get(location_col, "") if location_col else ""

            print(f"  [{idx+1}] Finding website for: {company_name}...")

            website_result = find_website(
                company_name=company_name,
                industry=industry if industry and str(industry) != "nan" else None,
                location=location if location and str(location) != "nan" else None,
                list_icp_context="startups",
                verbose=verbose
            )

            if website_result.get("found") and website_result.get("website"):
                df.at[idx, website_col] = website_result["website"]
                df.at[idx, "ai_website_found"] = True
                df.at[idx, "ai_website_confidence"] = website_result.get("confidence", 0)
                df.at[idx, "ai_website_match_type"] = website_result.get("match_type", "")
                websites_found += 1
                print(f"      Found: {website_result['website']} (conf: {website_result.get('confidence', 0):.2f})")
            else:
                df.at[idx, "ai_website_found"] = False
                print(f"      Not found")

    print(f"  Found {websites_found} missing websites")

    # Step 2: Qualify each company
    print(f"\n[2/5] Qualifying companies...")
    results = []
    qualified_domains = []

    for idx, row in df.iterrows():
        company_name = row.get(company_col, f"Company {idx}")
        website = row.get(website_col, "")
        description = row.get(desc_col, "") if desc_col else ""

        print(f"  [{idx+1}/{len(df)}] {company_name}...")

        qual_result = qualify_company(
            company_name=company_name,
            website=website,
            description=description,
            verbose=verbose
        )

        # Build result record
        record = {
            "company_name": company_name,
            "website": website,
            "original_description": description if description else "",

            # AI-generated website finder fields
            "ai_website_found": row.get("ai_website_found", False) if "ai_website_found" in df.columns else False,
            "ai_website_confidence": row.get("ai_website_confidence", 0) if "ai_website_confidence" in df.columns else 0,
            "ai_website_match_type": row.get("ai_website_match_type", "") if "ai_website_match_type" in df.columns else "",

            # AI-generated qualification fields
            "ai_is_qualified": qual_result.get("is_qualified", False),
            "ai_is_b2b": qual_result.get("is_b2b", False),
            "ai_confidence": qual_result.get("confidence", 0),
            "ai_business_type": qual_result.get("business_type", ""),
            "ai_reasoning": qual_result.get("reasoning", ""),
            "ai_disqualify_reason": qual_result.get("disqualify_reason", ""),
            "ai_cost_usd": qual_result.get("cost_usd", 0),
            "ai_error": qual_result.get("error", ""),

            # Metadata
            "_ai_generated_columns": "ai_website_found,ai_website_confidence,ai_website_match_type,ai_is_qualified,ai_is_b2b,ai_confidence,ai_business_type,ai_reasoning,ai_disqualify_reason"
        }

        results.append(record)

        # Track qualified domains for MX check
        if qual_result.get("is_qualified") and website and str(website) != "nan":
            domain = str(website).replace("http://", "").replace("https://", "").replace("www.", "").split("/")[0]
            if domain:
                qualified_domains.append(domain)

        status = "[OK] QUALIFIED" if qual_result.get("is_qualified") else "[X] DISQUALIFIED"
        print(f"      {status} (confidence: {qual_result.get('confidence', 0):.2f})")

    # Step 3: MX check on qualified companies
    print(f"\n[3/5] MX check on {len(qualified_domains)} qualified domains...")
    if qualified_domains:
        mx_results = check_mx_batch(qualified_domains)

        if "error" not in mx_results:
            # Add MX results to records
            for record in results:
                website = record.get("website", "")
                if not website or str(website) == "nan":
                    continue
                domain = str(website).replace("http://", "").replace("https://", "").replace("www.", "").split("/")[0]
                if domain in mx_results:
                    mx_data = mx_results[domain]
                    record["mx_has_gateway"] = mx_data.get("has_gateway", False)
                    record["mx_gateway_name"] = mx_data.get("gateway_name", "")
                    record["mx_gateway_severity"] = mx_data.get("gateway_severity", "")
                    record["mx_email_provider"] = mx_data.get("email_provider", "")
                    record["mx_safe_to_email"] = mx_data.get("safe_to_cold_email", True)

            print(f"  MX check complete")
        else:
            print(f"  MX check error: {mx_results.get('error')}")
    else:
        print("  No qualified companies to check")

    # Step 4: Push to Clay
    print(f"\n[4/5] Pushing {len(results)} records to Clay...")
    clay_result = push_to_clay(results, CLAY_WEBHOOK)
    print(f"  Result: {clay_result}")

    # Summary
    qualified_count = sum(1 for r in results if r.get("ai_is_qualified"))
    total_cost = sum(r.get("ai_cost_usd", 0) for r in results)

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total companies: {len(results)}")
    print(f"Qualified: {qualified_count}")
    print(f"Disqualified: {len(results) - qualified_count}")
    print(f"Total cost: ${total_cost:.4f}")
    print(f"Cost per company: ${total_cost/len(results):.4f}")

    # Save local copy
    output_path = Path(input_csv).parent / "qualification_results.csv"
    pd.DataFrame(results).to_csv(output_path, index=False)
    print(f"\nLocal copy saved to: {output_path}")

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test qualification system")
    parser.add_argument("--input", default="c:/Users/mitch/Desktop/Claude Code Projects/Specs/Business_Data_Staging/data/marketing_agencies_100.csv")
    parser.add_argument("--rows", type=int, default=10)
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    run_qualification_test(
        input_csv=args.input,
        max_rows=args.rows,
        verbose=args.verbose
    )
