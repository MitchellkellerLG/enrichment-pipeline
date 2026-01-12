"""
JSON Schema Definitions for Enrichment Pipeline v3.1

Defines structured output schemas for OpenRouter API.
These schemas ensure guaranteed JSON output from LLMs.

v3.1: Raw Data First - richer factual extraction, evidence required.
"""

# =============================================================================
# ICP Qualification Schema (v3.1 - Raw Data First)
# =============================================================================

ICP_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "icp_qualification",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "is_b2b": {
                    "type": ["boolean", "null"],
                    "description": "True if B2B, False if B2C, null if unclear"
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "description": "Confidence score 0.0-1.0"
                },
                "three_sentence_summary": {
                    "type": "string",
                    "description": "Sentence 1: what they do. Sentence 2: who they serve. Sentence 3: value proposition."
                },
                "primary_industry": {
                    "type": "string",
                    "description": "Specific industry (e.g., 'Staffing & Workforce Technology' not 'Technology')"
                },
                "what_they_do": {
                    "type": "string",
                    "description": "Core product/service in detail"
                },
                "who_they_serve": {
                    "type": "string",
                    "description": "Target customer profile with specifics"
                },
                "services_list": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Exact services/products as listed on website"
                },
                "industries_served": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "All industries mentioned on website"
                },
                "buyer_personas": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Target buyer titles/roles (e.g., 'VP Operations')"
                },
                "problems_they_solve": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of problems they solve"
                },
                "case_studies": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "url": {"type": "string"},
                            "snippet": {"type": "string"}
                        },
                        "required": ["title", "url"],
                        "additionalProperties": False
                    },
                    "description": "Case studies with title, URL, and first sentence"
                },
                "notable_customers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Known customers if mentioned"
                },
                "disqualify_reason": {
                    "type": ["string", "null"],
                    "description": "Reason for disqualification, or null"
                },
                "evidence": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "claim": {"type": "string"},
                            "url": {"type": "string"},
                            "snippet": {"type": "string"}
                        },
                        "required": ["claim", "url"],
                        "additionalProperties": False
                    },
                    "description": "Evidence supporting claims - required for every non-null fact"
                }
            },
            "required": [
                "is_b2b",
                "confidence",
                "three_sentence_summary",
                "primary_industry",
                "what_they_do",
                "who_they_serve",
                "evidence"
            ],
            "additionalProperties": False
        }
    }
}

ICP_REQUIRED_KEYS = ["is_b2b", "confidence", "three_sentence_summary", "primary_industry", "what_they_do", "who_they_serve"]


# =============================================================================
# Careers/LinkedIn Schema (v3.1 - Raw Data First with audit trail)
# =============================================================================

CAREERS_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "careers_linkedin",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "careers_url": {
                    "type": ["string", "null"],
                    "description": "URL to careers page"
                },
                "linkedin_url": {
                    "type": ["string", "null"],
                    "description": "LinkedIn company page URL"
                },
                "is_hiring": {
                    "type": "boolean",
                    "description": "True if actively hiring"
                },
                "hiring_intensity": {
                    "type": "string",
                    "enum": ["HOT", "WARM", "COLD"],
                    "description": "HOT=sales/marketing/leadership, WARM=technical, COLD=none"
                },
                "open_positions_count": {
                    "type": ["integer", "null"],
                    "description": "Number of open positions"
                },
                "all_job_titles": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "ALL job titles exactly as written on careers page"
                },
                "matched_roles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "matched_keyword": {"type": "string"}
                        },
                        "required": ["title", "matched_keyword"],
                        "additionalProperties": False
                    },
                    "description": "Roles that matched keywords with audit trail"
                },
                "keywords_used": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Keywords used for matching (for auditability)"
                },
                "evidence": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "claim": {"type": "string"},
                            "url": {"type": "string"},
                            "snippet": {"type": "string"}
                        },
                        "required": ["claim", "url"],
                        "additionalProperties": False
                    },
                    "description": "Evidence supporting claims"
                }
            },
            "required": [
                "is_hiring",
                "hiring_intensity",
                "all_job_titles",
                "evidence"
            ],
            "additionalProperties": False
        }
    }
}

CAREERS_REQUIRED_KEYS = ["is_hiring", "hiring_intensity", "all_job_titles"]


# =============================================================================
# News Intelligence Schema (v3.1 - Raw Data First with funding details)
# =============================================================================

NEWS_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "news_intelligence",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "has_recent_news": {
                    "type": "boolean",
                    "description": "True if any news found in last 12 months"
                },
                "has_funding_news": {
                    "type": "boolean",
                    "description": "True if funding news with evidence found"
                },
                "funding_round_type": {
                    "type": ["string", "null"],
                    "description": "Funding round type (e.g., Series A, Seed)"
                },
                "funding_amount": {
                    "type": ["string", "null"],
                    "description": "Funding amount (e.g., $10M)"
                },
                "funding_date": {
                    "type": ["string", "null"],
                    "description": "Funding announcement date"
                },
                "funding_investors": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of investors"
                },
                "funding_use_of_proceeds": {
                    "type": ["string", "null"],
                    "description": "Stated use of proceeds from funding article"
                },
                "funding_source_url": {
                    "type": ["string", "null"],
                    "description": "URL to funding news source"
                },
                "recent_announcements": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "headline": {"type": "string"},
                            "date": {"type": ["string", "null"]},
                            "type": {
                                "type": "string",
                                "enum": ["FUNDING", "PARTNERSHIP", "PRODUCT", "EXPANSION", "ACQUISITION", "LEADERSHIP", "AWARD", "OTHER"]
                            },
                            "source_url": {"type": "string"},
                            "three_sentence_summary": {"type": "string"}
                        },
                        "required": ["headline", "type", "source_url"],
                        "additionalProperties": False
                    },
                    "description": "All major announcements (not just funding)"
                },
                "outreach_timing": {
                    "type": "string",
                    "enum": ["POSITIVE", "NEUTRAL", "NEGATIVE"],
                    "description": "POSITIVE=funding/expansion/partnerships, NEUTRAL=none/awards, NEGATIVE=layoffs"
                },
                "search_queries_used": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Exact search queries used (for auditability)"
                }
            },
            "required": [
                "has_recent_news",
                "has_funding_news",
                "recent_announcements",
                "outreach_timing",
                "search_queries_used"
            ],
            "additionalProperties": False
        }
    }
}

NEWS_REQUIRED_KEYS = ["has_recent_news", "has_funding_news", "recent_announcements", "outreach_timing", "search_queries_used"]


# =============================================================================
# Default Specs (for Spec Builder fallback)
# =============================================================================

DEFAULT_ICP_SPEC = {
    "hard_gates": [
        {"gate_id": "is_b2b", "rule": "Must sell to businesses", "weight": "HARD"}
    ],
    "required_signals": [
        {"signal_id": "has_pricing", "rule": "Public pricing or 'Request Demo'"},
        {"signal_id": "has_customers", "rule": "Customer logos or case studies"}
    ],
    "signals_threshold": 2,
    "b2b_policy": "HARD_GATE",
    "scoring_rubric": "1-10 based on signals met + evidence quality"
}

DEFAULT_HIRING_SPEC = {
    "mode": "GROWTH_ROLES_ONLY",
    "role_keywords": ["Account Executive", "Sales", "Growth", "Marketing", "Head of", "VP", "Director"],
    "intensity_rules": {
        "HOT": "Hiring sales/marketing/leadership",
        "WARM": "Hiring technical roles only",
        "COLD": "No open positions"
    },
    "max_roles_to_return": 10
}

DEFAULT_NEWS_SPEC = {
    "lookback_days": 365,
    "search_plan": {
        "search_a": {
            "name": "funding_only",
            "query_template": '"{company_name}" (raised OR funding OR "series a" OR "series b" OR seed OR investment)',
            "max_results": 5
        },
        "search_b": {
            "name": "other_signals",
            "query_template": '"{company_name}" (acquired OR acquisition OR layoffs OR partnership OR launch OR expansion) -funding -series -raised',
            "max_results": 3
        }
    },
    "funding_evidence_requirements": ["amount", "round_type", "investor", "date"],
    "funding_min_attributes": 1
}

DEFAULT_OUTPUT_SPEC = {
    "company": {
        "include_summary": True,
        "required_fields": ["what_they_do", "who_they_serve"],
        "optional_fields": ["industries", "notable_customers"]
    },
    "icp": {
        "required_fields": ["is_b2b", "confidence"],
        "optional_fields": ["disqualify_reason"]
    },
    "hiring": {
        "required_fields": ["is_hiring", "hiring_intensity"],
        "optional_fields": ["open_positions_count", "notable_roles"],
        "max_roles": 10
    },
    "news": {
        "required_fields": ["has_recent_news", "outreach_timing", "news_items"],
        "optional_fields": ["sales_importance", "funding_use_of_proceeds"],
        "max_items": 5
    }
}


# =============================================================================
# Helper Functions
# =============================================================================

def get_schema_for_module(module_name: str) -> dict:
    """Get the response_format schema for a module."""
    schemas = {
        "icp": ICP_SCHEMA,
        "careers": CAREERS_SCHEMA,
        "news": NEWS_SCHEMA
    }
    return schemas.get(module_name)


def get_required_keys_for_module(module_name: str) -> list:
    """Get required keys for validation."""
    keys = {
        "icp": ICP_REQUIRED_KEYS,
        "careers": CAREERS_REQUIRED_KEYS,
        "news": NEWS_REQUIRED_KEYS
    }
    return keys.get(module_name, [])
