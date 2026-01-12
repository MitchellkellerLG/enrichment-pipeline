# ICP Qualification Prompt v3.1

## Version History
- v3.1 (2026-01-11): Raw Data First - extract facts, not opinions
- v3.0: Initial multi-agent split

## Prompt

```
# LeadGrow ICP Qualification

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
Company Name: {company_name}
Website: {website}
```

## Required Keys
- is_b2b
- confidence
- three_sentence_summary
- primary_industry
- what_they_do
- who_they_serve

## Tools
- scrape_url
- web_search

## Config
- max_turns: 5
- max_retries: 2

## Known Failure Modes
_Add constraints here as we learn from failures_

1. **Empty response on first try** - Gemini sometimes returns empty when site blocks scraping
   - Fix: Retry logic with GPT fallback
