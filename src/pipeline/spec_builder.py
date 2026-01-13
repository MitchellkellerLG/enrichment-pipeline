"""
Spec Builder Agent for Enrichment Pipeline v3

Generates campaign-specific specs based on ICP goal and requirements.
Outputs: icp_spec, output_spec, hiring_spec, news_spec

Usage:
    from spec_builder import build_specs
    specs = build_specs(icp_goal="B2B SaaS companies doing AI/ML")
"""

import json
import sys
from pathlib import Path

from src.schemas.definitions import (
    DEFAULT_ICP_SPEC,
    DEFAULT_HIRING_SPEC,
    DEFAULT_NEWS_SPEC,
    DEFAULT_OUTPUT_SPEC
)


# =============================================================================
# Spec Builder Prompts
# =============================================================================

SPEC_BUILDER_PROMPT = """# Spec Builder Agent

You are building configuration specs for a B2B lead qualification pipeline.

## Your Task
Generate JSON specs based on the ICP goal provided.

## ICP Goal
{icp_goal}

## Required Outputs
{required_outputs}

## Generate These Specs

### 1. icp_spec
Define qualification rules:
- hard_gates: Requirements that MUST be met (like "must be B2B")
- required_signals: Nice-to-have signals (like "has pricing page")
- signals_threshold: How many signals needed to qualify
- b2b_policy: HARD_GATE (must be B2B) or SOFT (B2B preferred)

### 2. hiring_spec
Define hiring signal rules:
- mode: ANY_HIRING, GROWTH_ROLES_ONLY, or CUSTOM_ROLE_MATCH
- role_keywords: What roles indicate growth (sales, marketing, leadership)
- intensity_rules: HOT/WARM/COLD definitions

### 3. news_spec
Define news gathering rules:
- lookback_days: 365 (default)
- search_plan: Funding-first search + other signals search

### 4. output_spec
Define what fields to include:
- required_fields per module
- max_items for lists

## Output Format
Return a single JSON object with all 4 specs:

```json
{{
  "icp_spec": {{...}},
  "hiring_spec": {{...}},
  "news_spec": {{...}},
  "output_spec": {{...}}
}}
```

CRITICAL: Return ONLY valid JSON. No explanations before or after.
"""


def extract_json_object(text: str) -> str | None:
    """Extract first JSON object from text."""
    import re
    if not text:
        return None
    text = text.strip()

    # Strip markdown fences
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text, flags=re.IGNORECASE)

    # Find first {...} by brace matching
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

    return None


def safe_json_loads(text: str) -> dict | None:
    """Safely parse JSON from LLM output."""
    raw = extract_json_object(text)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def validate_specs(specs: dict) -> dict:
    """
    Validate that all required spec keys are present.
    Returns {"valid": True} or {"valid": False, "errors": [...]}
    """
    errors = []
    required_specs = ["icp_spec", "hiring_spec", "news_spec", "output_spec"]

    for spec_name in required_specs:
        if spec_name not in specs:
            errors.append(f"Missing required spec: {spec_name}")

    if errors:
        return {"valid": False, "errors": errors}
    return {"valid": True}


def build_specs(
    icp_goal: str = None,
    required_outputs: list = None,
    verbose: bool = False,
    max_retries: int = 3
) -> dict:
    """
    Build campaign specs based on ICP goal.

    Args:
        icp_goal: Natural language description of ideal customer profile
        required_outputs: List of fields needed (default: standard set)
        verbose: Print progress
        max_retries: Number of retries before falling back to defaults

    Returns:
        dict with icp_spec, hiring_spec, news_spec, output_spec
    """
    from call_llm_openrouter import run_agent

    # Use defaults if no goal provided
    if not icp_goal:
        if verbose:
            print("No ICP goal provided, using default specs")
        return {
            "icp_spec": DEFAULT_ICP_SPEC,
            "hiring_spec": DEFAULT_HIRING_SPEC,
            "news_spec": DEFAULT_NEWS_SPEC,
            "output_spec": DEFAULT_OUTPUT_SPEC,
            "source": "defaults"
        }

    if not required_outputs:
        required_outputs = [
            "is_b2b", "confidence", "what_they_do", "who_they_serve",
            "hiring_intensity", "open_positions_count",
            "has_funding_news", "outreach_timing", "ice_breaker"
        ]

    prompt = SPEC_BUILDER_PROMPT.format(
        icp_goal=icp_goal,
        required_outputs=", ".join(required_outputs)
    )

    for attempt in range(max_retries):
        if verbose:
            print(f"Spec builder attempt {attempt + 1}/{max_retries}")

        result = run_agent(
            prompt=prompt,
            tools=[],  # No tools needed - just generate JSON
            max_turns=1,
            verbose=verbose
        )

        if "error" in result:
            if verbose:
                print(f"  Error: {result['error']}")
            continue

        content = result.get("content", "")
        specs = safe_json_loads(content)

        if not specs:
            if verbose:
                print("  Failed to parse JSON")
            continue

        validation = validate_specs(specs)
        if not validation["valid"]:
            if verbose:
                print(f"  Validation failed: {validation['errors']}")
            continue

        # Success!
        specs["source"] = "generated"
        specs["cost_usd"] = result.get("costs", {}).get("total_usd", 0)
        return specs

    # All retries failed - use defaults
    if verbose:
        print("Spec generation failed, using defaults")
    return {
        "icp_spec": DEFAULT_ICP_SPEC,
        "hiring_spec": DEFAULT_HIRING_SPEC,
        "news_spec": DEFAULT_NEWS_SPEC,
        "output_spec": DEFAULT_OUTPUT_SPEC,
        "source": "defaults_after_failure"
    }


def get_default_specs() -> dict:
    """Return default specs without LLM call."""
    return {
        "icp_spec": DEFAULT_ICP_SPEC,
        "hiring_spec": DEFAULT_HIRING_SPEC,
        "news_spec": DEFAULT_NEWS_SPEC,
        "output_spec": DEFAULT_OUTPUT_SPEC,
        "source": "defaults"
    }


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate campaign specs")
    parser.add_argument("--goal", "-g", help="ICP goal description")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--output", "-o", help="Output file path")

    args = parser.parse_args()

    specs = build_specs(
        icp_goal=args.goal,
        verbose=args.verbose
    )

    if args.output:
        Path(args.output).write_text(json.dumps(specs, indent=2), encoding="utf-8")
        print(f"Specs saved to: {args.output}")
    else:
        print(json.dumps(specs, indent=2))
