"""
OpenRouter LLM Tool Calling - Execute prompts with tool access.

Autonomous agent that can search the web and scrape content using
OpenRouter-compatible LLMs with tool calling.

Usage:
    python call_llm_openrouter.py "Research Acme Corp"
    python call_llm_openrouter.py "Research Acme Corp" --model deepseek/deepseek-r1
    python call_llm_openrouter.py prompt.txt --max-turns 10 -o results.json
    python call_llm_openrouter.py "Enrich leads" --tools spider --verbose

Environment:
    OPENROUTER_API_KEY - Required. Get from https://openrouter.ai/keys
"""

import argparse
import io
import json
import os
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

load_dotenv()

API_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Base path for tool scripts
TOOLS_DIR = Path(__file__).parent
SPIDER_DIR = TOOLS_DIR / "spider"
FIRECRAWL_DIR = TOOLS_DIR / "firecrawl"

# =============================================================================
# Self-Annealing Protected Sites System
# =============================================================================

PROTECTED_SITES_FILE = TOOLS_DIR / "protected_sites.json"
FAILURE_THRESHOLD = 3  # Failures before a site is "learned" as protected
BASE_PROTECTED_SITES = ["crunchbase.com", "linkedin.com", "zoominfo.com", "pitchbook.com"]

# Indicators that a scrape was blocked
BLOCKING_INDICATORS = [
    "cloudflare",
    "just a moment",
    "checking your browser",
    "verify you are human",
    "access denied",
    "403 forbidden",
    "blocked",
    "captcha",
]


def load_protected_sites() -> dict:
    """Load the protected sites registry."""
    if PROTECTED_SITES_FILE.exists():
        try:
            return json.loads(PROTECTED_SITES_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_protected_sites(sites: dict) -> None:
    """Save the protected sites registry."""
    PROTECTED_SITES_FILE.write_text(json.dumps(sites, indent=2), encoding="utf-8")


def get_learned_domains() -> list:
    """Get list of domains that have been learned as protected."""
    sites = load_protected_sites()
    learned = [domain for domain, info in sites.items() if info.get("learned", False)]
    return learned


def log_scrape_failure(url: str, reason: str) -> bool:
    """
    Log a scrape failure for a domain. Returns True if domain is now learned.
    """
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.lower().replace("www.", "")

    sites = load_protected_sites()

    if domain not in sites:
        sites[domain] = {"failures": 0, "reasons": [], "learned": False}

    sites[domain]["failures"] += 1
    if reason not in sites[domain]["reasons"]:
        sites[domain]["reasons"].append(reason)

    # Check if threshold reached
    if sites[domain]["failures"] >= FAILURE_THRESHOLD and not sites[domain]["learned"]:
        sites[domain]["learned"] = True
        save_protected_sites(sites)
        return True  # Newly learned

    save_protected_sites(sites)
    return False


def get_all_protected_domains() -> list:
    """Get all protected domains (base + learned)."""
    learned = get_learned_domains()
    all_domains = list(set(BASE_PROTECTED_SITES + learned))
    return sorted(all_domains)


def get_firecrawl_description() -> str:
    """Generate dynamic firecrawl_scrape description with all protected sites."""
    domains = get_all_protected_domains()
    domains_str = ", ".join(domains)
    return f"Scrape a URL using Firecrawl (anti-bot resistant). Use this when scrape_url fails due to Cloudflare or other blocking. Works on protected sites like {domains_str}."


def detect_blocking(content: str) -> str | None:
    """Detect if content indicates blocking. Returns reason or None."""
    content_lower = content.lower()

    # Check for blocking indicators
    for indicator in BLOCKING_INDICATORS:
        if indicator in content_lower:
            return indicator

    # Check for suspiciously short content
    if len(content.strip()) < 500:
        return "content_too_short"

    return None

# =============================================================================
# Default System Prompt
# =============================================================================

DEFAULT_SYSTEM_PROMPT = """
# Role
You are a research agent. Given a task, you scrape websites and extract structured data.

# Tools
- scrape_url: Extract page content (returns markdown with embedded links)
- firecrawl_scrape: Anti-bot scraping (use if scrape_url blocked)
- web_search: Google search (for LinkedIn profiles or finding websites)

# How to Work

1. UNDERSTAND: What data is needed?

2. SCRAPE HOMEPAGE: Start by scraping the provided URL
   - The markdown will contain links like: [About Us](/about), [Team](/team)

3. FIND TARGET PAGE: Look at the links in the scraped content
   - Team data? Look for: /team, /about, /people, /leadership
   - Pricing? Look for: /pricing, /plans
   - Jobs? Look for: /careers, /jobs

4. SCRAPE TARGET: Scrape the most promising link

5. ENRICH (if needed): For LinkedIn profiles, search:
   "[Name] [Title] [Company] site:linkedin.com"

# Rules
- Maximum {max_steps} steps
- If scrape blocked -> try firecrawl_scrape
- For LinkedIn -> search only, never scrape
- Stop when you have sufficient data
- Always return structured JSON

# Protected Sites (use firecrawl_scrape):
{protected_sites}
"""


def build_system_prompt(max_steps: int = 5, custom_additions: str = None) -> str:
    """Build the default system prompt with dynamic values."""
    protected = get_all_protected_domains()

    prompt = DEFAULT_SYSTEM_PROMPT.format(
        max_steps=max_steps,
        protected_sites=", ".join(protected)
    )

    if custom_additions:
        prompt += f"\n\n# Additional Instructions\n{custom_additions}"

    return prompt


# =============================================================================
# Tool Definitions (OpenAI-compatible format)
# =============================================================================

TOOL_DEFINITIONS = {
    "scrape_url": {
        "type": "function",
        "function": {
            "name": "scrape_url",
            "description": "Extract content from a single URL. Returns clean markdown content optimized for reading. Use this when you need to get the full content of a webpage.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "The full URL to scrape (must include https://)"
                    },
                    "selector": {
                        "type": "string",
                        "description": "Optional CSS selector to extract specific content (e.g., 'article', '.main-content')"
                    }
                },
                "required": ["url"]
            }
        }
    },
    "web_search": {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for information. Returns URLs, titles, and snippets from search results. Use this to find relevant URLs before scraping them.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query"
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Number of results to return (default: 10, max: 20)"
                    }
                },
                "required": ["query"]
            }
        }
    },
    "take_screenshot": {
        "type": "function",
        "function": {
            "name": "take_screenshot",
            "description": "Capture a screenshot of a webpage. Returns the path to the saved image file.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "The URL to screenshot"
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Path to save the screenshot (e.g., 'screenshot.png')"
                    },
                    "full_page": {
                        "type": "boolean",
                        "description": "Capture the entire scrollable page (default: true)"
                    }
                },
                "required": ["url", "output_path"]
            }
        }
    },
    "firecrawl_scrape": {
        "type": "function",
        "function": {
            "name": "firecrawl_scrape",
            "description": "Scrape a URL using Firecrawl (anti-bot resistant). Use this when scrape_url fails due to Cloudflare or other blocking. Works on protected sites like crunchbase.com, linkedin.com, zoominfo.com, pitchbook.com.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "The full URL to scrape"
                    }
                },
                "required": ["url"]
            }
        }
    }
}

# Tool sets for --tools flag
TOOL_SETS = {
    "all": list(TOOL_DEFINITIONS.keys()),
    "websearch": ["web_search"],
    "scrape": ["scrape_url", "firecrawl_scrape"],
    "firecrawl": ["firecrawl_scrape"],
}


def get_tool_definitions(tool_names: list = None) -> list:
    """
    Get tool definitions with dynamic firecrawl description.
    The firecrawl_scrape description includes learned protected sites.
    """
    import copy
    if tool_names is None:
        tool_names = TOOL_SETS["all"]

    result = []
    for name in tool_names:
        if name not in TOOL_DEFINITIONS:
            continue
        tool_def = copy.deepcopy(TOOL_DEFINITIONS[name])

        # Inject dynamic description for firecrawl_scrape
        if name == "firecrawl_scrape":
            tool_def["function"]["description"] = get_firecrawl_description()

        result.append(tool_def)

    return result


# =============================================================================
# Tool Execution
# =============================================================================

def execute_tool(name: str, arguments: dict, verbose: bool = False) -> str:
    """Execute a tool and return the result as a string."""

    if verbose:
        print(f"  [TOOL] {name}: {json.dumps(arguments, indent=2)}")

    try:
        if name == "scrape_url":
            return _execute_scrape(arguments)
        elif name == "web_search":
            return _execute_search(arguments)
        elif name == "take_screenshot":
            return _execute_screenshot(arguments)
        elif name == "firecrawl_scrape":
            return _execute_firecrawl_scrape(arguments)
        else:
            return f"Error: Unknown tool '{name}'"
    except Exception as e:
        return f"Error executing {name}: {str(e)}"


def _execute_scrape(args: dict) -> str:
    """Execute spider scrape.py with failure detection for self-annealing."""
    cmd = [
        sys.executable,
        str(SPIDER_DIR / "scrape.py"),
        args["url"]
    ]
    if args.get("selector"):
        cmd.extend(["--selector", args["selector"]])

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace")

    if result.returncode != 0:
        # Log failure for potential learning
        log_scrape_failure(args["url"], "error")
        return f"Error: {result.stderr}"

    content = result.stdout or ""

    # Detect blocking and log for self-annealing
    blocking_reason = detect_blocking(content)
    if blocking_reason:
        newly_learned = log_scrape_failure(args["url"], blocking_reason)
        if newly_learned:
            # Append hint to response so LLM knows this site is now learned
            content += f"\n\n[SYSTEM: This domain has been added to protected sites list. Use firecrawl_scrape for future requests.]"

    return content


def _execute_search(args: dict) -> str:
    """Execute serper_search.py"""
    cmd = [
        sys.executable,
        str(TOOLS_DIR / "serper_search.py"),
        args["query"]
    ]
    # Note: serper_search.py doesn't support --num, uses default 10 results

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, encoding="utf-8", errors="replace")

    if result.returncode != 0:
        return f"Error: {result.stderr}"
    return result.stdout or ""


def _execute_screenshot(args: dict) -> str:
    """Execute spider screenshot.py"""
    cmd = [
        sys.executable,
        str(SPIDER_DIR / "screenshot.py"),
        args["url"],
        "-o", args["output_path"]
    ]
    if args.get("full_page") is False:
        cmd.append("--no-full-page")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace")

    if result.returncode != 0:
        return f"Error: {result.stderr}"
    return f"Screenshot saved to: {args['output_path']}"


def _execute_firecrawl_scrape(args: dict) -> str:
    """Execute firecrawl scrape.py (anti-bot resistant)"""
    cmd = [
        sys.executable,
        str(FIRECRAWL_DIR / "scrape.py"),
        args["url"],
        "--json"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace")

    if result.returncode != 0:
        return f"Error: {result.stderr}"
    return result.stdout or ""


# =============================================================================
# OpenRouter API
# =============================================================================

# Default model and fallback chain
DEFAULT_MODEL = "google/gemini-2.5-flash-lite"
FALLBACK_MODELS = [
    "google/gemini-2.5-flash-lite",
    "deepseek/deepseek-chat",
    "openai/gpt-oss-120b",  # OSS model - cheaper, good quality
]


def call_openrouter(
    messages: list,
    tools: list,
    model: str,
    temperature: float = 0.7,
    fallback: bool = False,
    response_format: dict = None
) -> dict:
    """
    Call OpenRouter API with tool definitions and optional fallback.

    Args:
        messages: Chat messages
        tools: Tool definitions
        model: Model to use
        temperature: Sampling temperature
        fallback: If True, use fallback routing
        response_format: Optional structured output format. Example:
            {
                "type": "json_schema",
                "json_schema": {
                    "name": "icp_result",
                    "strict": True,
                    "schema": {...}
                }
            }
    """

    if not OPENROUTER_API_KEY:
        raise ValueError("OPENROUTER_API_KEY not set in environment")

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://leadgrow.ai",
        "X-Title": "LeadGrow Enrichment Agent"
    }

    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }

    # Enable fallback routing if requested
    if fallback:
        payload["route"] = "fallback"
        payload["models"] = FALLBACK_MODELS

    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    # Add structured output format if specified
    if response_format:
        payload["response_format"] = response_format

    response = requests.post(API_URL, headers=headers, json=payload, timeout=120)
    response.raise_for_status()

    return response.json()


# =============================================================================
# Agent Loop
# =============================================================================

# Model pricing per token (input, output)
MODEL_PRICING = {
    "openai/gpt-4o-mini": (0.00000015, 0.0000006),
    "qwen/qwen-2.5-72b-instruct": (0.00000029, 0.00000039),
    "deepseek/deepseek-chat": (0.00000014, 0.00000028),
    "x-ai/grok-4.1-fast": (0.000003, 0.000015),
    "google/gemini-2.5-flash-lite": (0.000000075, 0.0000003),
    "openai/gpt-oss-120b": (0.00000004, 0.00000015),
    "meta-llama/llama-3.3-70b-instruct": (0.00000039, 0.00000039),
}


def _build_costs(model: str, prompt_tokens: int, completion_tokens: int,
                 serper_credits: int, spider_credits: float) -> dict:
    """Build cost breakdown."""
    inp_price, out_price = MODEL_PRICING.get(model, (0.0000005, 0.0000015))

    llm_cost = (prompt_tokens * inp_price) + (completion_tokens * out_price)
    serper_cost = serper_credits * 0.00075  # $0.00075 per credit
    spider_cost = spider_credits / 10000  # 10k credits = $1

    return {
        "llm": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "cost_usd": round(llm_cost, 8)
        },
        "serper": {
            "credits": serper_credits,
            "cost_usd": round(serper_cost, 6)
        },
        "spider": {
            "credits": round(spider_credits, 2),
            "cost_usd": round(spider_cost, 8)
        },
        "total_usd": round(llm_cost + serper_cost + spider_cost, 6)
    }


def extract_costs_from_output(tool_name: str, output: str) -> dict:
    """Extract costs from tool output."""
    import re
    costs = {}

    if tool_name == "web_search":
        # Serper returns {"credits": N}
        match = re.search(r'"credits":\s*(\d+)', output)
        if match:
            costs["serper_credits"] = int(match.group(1))
            costs["serper_cost_usd"] = int(match.group(1)) * 0.00075  # $0.00075 per credit

    elif tool_name == "scrape_url":
        # Spider returns [Cost: X credits]
        match = re.search(r'\[Cost:\s*([\d.]+)\s*credits\]', output)
        if match:
            costs["spider_credits"] = float(match.group(1))
            costs["spider_cost_usd"] = float(match.group(1)) / 10000  # 10k credits = $1

    return costs


# =============================================================================
# Schema Validation & Retry Logic
# =============================================================================

def validate_schema(data: dict, required_keys: list, evidence_required: bool = False) -> dict:
    """
    Validate parsed JSON against required schema.

    Args:
        data: Parsed JSON dict
        required_keys: List of keys that must be present
        evidence_required: If True, check that 'evidence' key has items

    Returns:
        {"valid": True} or {"valid": False, "errors": [...]}
    """
    errors = []

    for key in required_keys:
        if key not in data:
            errors.append(f"Missing required key: {key}")
        elif data[key] is None:
            # None is OK for optional fields, but we track it
            pass

    if evidence_required:
        evidence = data.get("evidence", [])
        if not evidence or len(evidence) == 0:
            errors.append("Evidence required but empty or missing")

    if errors:
        return {"valid": False, "errors": errors}
    return {"valid": True}


def run_agent_with_retry(
    prompt: str,
    model: str = DEFAULT_MODEL,
    max_turns: int = 5,
    tools: list = None,
    verbose: bool = False,
    system_prompt: str = None,
    fallback: bool = False,
    response_format: dict = None,
    required_keys: list = None,
    evidence_required: bool = False,
    max_retries: int = 2,
    fallback_model: str = "openai/gpt-4o-mini"  # OSS-120b struggles with JSON output
) -> dict:
    """
    Run agent with automatic retry on parse/validation failure.

    Args:
        prompt: Task prompt
        model: Primary model
        max_turns: Max conversation turns
        tools: Tool list
        verbose: Print progress
        system_prompt: Custom system prompt
        fallback: Use OpenRouter fallback routing
        response_format: Structured output format
        required_keys: Keys that must be present in response
        evidence_required: Require evidence array with items
        max_retries: Number of retries on failure
        fallback_model: Model to use on retry #2

    Returns:
        dict with 'content', 'parsed', 'validation', etc.
    """
    import re

    def extract_json_object(text: str) -> str | None:
        """Extract first JSON object from text."""
        if not text:
            return None
        text = text.strip()

        # Strip Python code wrappers
        text = re.sub(r"<tool_code>.*?data\s*=\s*", "", text, flags=re.DOTALL)
        text = re.sub(r"print\(.*?\)\s*'''.*$", "", text, flags=re.DOTALL)

        # Look for JSON inside markdown fence
        fence_match = re.search(r"```(?:json)?\s*(\{[^`]*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if fence_match:
            return fence_match.group(1)

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

    last_error = None
    total_cost = 0

    for attempt in range(max_retries + 1):
        current_model = model if attempt < max_retries else fallback_model

        if verbose and attempt > 0:
            print(f"  [RETRY {attempt}/{max_retries}] Using model: {current_model}")

        result = run_agent(
            prompt=prompt,
            model=current_model,
            max_turns=max_turns,
            tools=tools,
            verbose=verbose,
            system_prompt=system_prompt,
            fallback=fallback,
            response_format=response_format
        )

        # Track costs across retries
        total_cost += result.get("costs", {}).get("total_usd", 0)

        if "error" in result:
            last_error = result["error"]
            if verbose:
                print(f"  [ERROR] {last_error}")
            continue

        content = result.get("content", "")

        # Try to parse JSON
        parsed = safe_json_loads(content)
        if not parsed:
            last_error = "Could not parse JSON from response"
            if verbose:
                print(f"  [PARSE ERROR] {last_error}")
            continue

        # Validate schema if required_keys specified
        if required_keys:
            validation = validate_schema(parsed, required_keys, evidence_required)
            if not validation["valid"]:
                last_error = f"Schema validation failed: {validation['errors']}"
                if verbose:
                    print(f"  [VALIDATION ERROR] {last_error}")
                continue
        else:
            validation = {"valid": True}

        # Success!
        result["parsed"] = parsed
        result["validation"] = validation
        result["attempts"] = attempt + 1
        result["costs"]["total_usd"] = total_cost
        return result

    # All retries failed
    return {
        "error": last_error or "Unknown error after retries",
        "attempts": max_retries + 1,
        "costs": {"total_usd": total_cost}
    }


def run_agent(
    prompt: str,
    model: str = DEFAULT_MODEL,
    max_turns: int = 5,
    tools: list = None,
    verbose: bool = False,
    system_prompt: str = None,
    fallback: bool = False,
    response_format: dict = None
) -> dict:
    """
    Run the agent loop with tool calling.

    Args:
        prompt: The task prompt
        model: Model to use
        max_turns: Maximum conversation turns
        tools: List of tool names to enable
        verbose: Print progress
        system_prompt: Custom system prompt
        fallback: If True, use OpenRouter's fallback routing with FALLBACK_MODELS
        response_format: Optional structured output format for guaranteed JSON. Example:
            {
                "type": "json_schema",
                "json_schema": {
                    "name": "result",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {...},
                        "required": [...],
                        "additionalProperties": False
                    }
                }
            }

    Returns:
        dict with 'content' (final response), 'messages' (full history),
        'turns' (number of turns used), 'tools_called' (list of tool calls),
        'costs' (breakdown of LLM, serper, spider costs)
    """

    # Enforce step constraints: hard cap at 10
    max_turns = min(max_turns, 10)

    # Use default system prompt if none provided
    if system_prompt is None:
        system_prompt = build_system_prompt(max_steps=max_turns)

    # Build tool list with dynamic descriptions (includes learned protected sites)
    tool_defs = get_tool_definitions(tools or TOOL_SETS["all"])

    # Build messages
    messages = []
    messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    tools_called = []

    # Cost tracking
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_serper_credits = 0
    total_spider_credits = 0

    for turn in range(max_turns):
        if verbose:
            print(f"\n[Turn {turn + 1}/{max_turns}]")

        # Inject step warning when approaching limit
        if turn >= max_turns - 2 and turn > 0:
            messages.append({
                "role": "system",
                "content": f"You are on step {turn + 1} of {max_turns}. Wrap up and synthesize your findings."
            })

        # Call LLM
        response = call_openrouter(messages, tool_defs, model, fallback=fallback, response_format=response_format)

        if "error" in response:
            return {
                "error": response["error"],
                "messages": messages,
                "turns": turn + 1,
                "tools_called": tools_called,
                "costs": _build_costs(model, total_prompt_tokens, total_completion_tokens,
                                      total_serper_credits, total_spider_credits)
            }

        # Track token usage from response
        usage = response.get("usage", {})
        total_prompt_tokens += usage.get("prompt_tokens", 0)
        total_completion_tokens += usage.get("completion_tokens", 0)

        assistant_message = response["choices"][0]["message"]

        # Check for tool calls
        tool_calls = assistant_message.get("tool_calls", [])

        if not tool_calls:
            # No tool calls - final response
            if verbose:
                print(f"  [FINAL] {assistant_message.get('content', '')[:200]}...")

            return {
                "content": assistant_message.get("content", ""),
                "messages": messages + [assistant_message],
                "turns": turn + 1,
                "tools_called": tools_called,
                "costs": _build_costs(model, total_prompt_tokens, total_completion_tokens,
                                      total_serper_credits, total_spider_credits)
            }

        # Execute tool calls
        messages.append(assistant_message)

        for tool_call in tool_calls:
            func_name = tool_call["function"]["name"]
            raw_args = tool_call["function"]["arguments"]

            # Parse arguments with error handling for malformed JSON
            try:
                func_args = json.loads(raw_args)
            except json.JSONDecodeError:
                # Try to fix common JSON issues
                try:
                    import re
                    fixed = raw_args.strip()

                    # Fix missing braces (common with OSS models)
                    # If starts with " but not {, add opening brace
                    if fixed.startswith('"') and not fixed.startswith('{'):
                        fixed = '{' + fixed
                    # If ends with " or number but not }, add closing brace
                    if not fixed.endswith('}') and (fixed.endswith('"') or fixed[-1].isdigit()):
                        fixed = fixed + '}'

                    # Remove trailing commas
                    fixed = re.sub(r',\s*}', '}', fixed)
                    fixed = re.sub(r',\s*]', ']', fixed)

                    func_args = json.loads(fixed)
                    if verbose:
                        print(f"  [FIXED] Auto-repaired malformed JSON")
                except json.JSONDecodeError:
                    if verbose:
                        print(f"  [ERROR] Malformed JSON in tool call: {raw_args[:100]}...")
                    # Return error to model so it can retry
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call["id"],
                        "content": f"Error: Malformed JSON in arguments. Please format as valid JSON. Raw: {raw_args[:200]}"
                    })
                    continue

            if verbose:
                print(f"  [CALLING] {func_name}")

            result = execute_tool(func_name, func_args, verbose)

            # Extract costs from tool output
            tool_costs = extract_costs_from_output(func_name, result)
            total_serper_credits += tool_costs.get("serper_credits", 0)
            total_spider_credits += tool_costs.get("spider_credits", 0)

            tools_called.append({
                "name": func_name,
                "arguments": func_args,
                "result_length": len(result),
                "costs": tool_costs
            })

            # Add tool result to messages
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "content": result
            })

    # Max turns exceeded
    return {
        "error": f"Max turns ({max_turns}) exceeded",
        "messages": messages,
        "turns": max_turns,
        "tools_called": tools_called,
        "costs": _build_costs(model, total_prompt_tokens, total_completion_tokens,
                              total_serper_credits, total_spider_credits)
    }


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="OpenRouter LLM with tool calling for web research",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python call_llm_openrouter.py "Research Acme Corp and list their products"
  python call_llm_openrouter.py "Find and summarize the pricing page of stripe.com"
  python call_llm_openrouter.py prompt.txt --model deepseek/deepseek-r1 -o result.json
  python call_llm_openrouter.py "Enrich lead: company.com" --tools scrape --verbose

Available tool sets:
  all       - All tools (default)
  websearch - web_search only (Serper)
  scrape    - scrape_url, firecrawl_scrape

Available models (examples):
  google/gemini-2.5-flash-lite (default, cheapest, fast)
  deepseek/deepseek-chat (cheap, good quality)
  openai/gpt-4o-mini (reliable, moderate cost)
  qwen/qwen-2.5-72b-instruct (good, moderate cost)

Fallback routing:
  Use --fallback to auto-failover: Gemini -> DeepSeek -> GPT-4o-mini
        """
    )

    parser.add_argument(
        "prompt",
        help="Prompt string or path to prompt file (.txt)"
    )
    parser.add_argument(
        "--model", "-m",
        default=DEFAULT_MODEL,
        help=f"Model to use (default: {DEFAULT_MODEL})"
    )
    parser.add_argument(
        "--fallback", "-f",
        action="store_true",
        help="Enable fallback routing: Gemini -> DeepSeek -> GPT-4o-mini"
    )
    parser.add_argument(
        "--max-turns", "-t",
        type=int,
        default=5,
        help="Maximum conversation turns (default: 5)"
    )
    parser.add_argument(
        "--tools",
        default="all",
        help="Tool set: all, websearch, scrape, firecrawl (default: all)"
    )
    parser.add_argument(
        "--system", "-s",
        help="System prompt or path to system prompt file"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file path (saves full JSON result)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of just content"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show tool calls and progress"
    )

    args = parser.parse_args()

    # Check API key
    if not OPENROUTER_API_KEY:
        print("Error: OPENROUTER_API_KEY not set in environment")
        print("Get your key at: https://openrouter.ai/keys")
        sys.exit(1)

    # Load prompt
    if Path(args.prompt).exists():
        prompt = Path(args.prompt).read_text(encoding="utf-8")
    else:
        prompt = args.prompt

    # Load system prompt
    system_prompt = None
    if args.system:
        if Path(args.system).exists():
            system_prompt = Path(args.system).read_text(encoding="utf-8")
        else:
            system_prompt = args.system

    # Get tool list
    if args.tools in TOOL_SETS:
        tools = TOOL_SETS[args.tools]
    else:
        # Custom comma-separated list
        tools = [t.strip() for t in args.tools.split(",")]

    if args.verbose:
        print(f"Model: {args.model}")
        if args.fallback:
            print(f"Fallback: {' -> '.join(FALLBACK_MODELS)}")
        print(f"Tools: {tools}")
        print(f"Max turns: {args.max_turns}")
        print(f"Prompt: {prompt[:100]}...")

    # Run agent
    result = run_agent(
        prompt=prompt,
        model=args.model,
        max_turns=args.max_turns,
        tools=tools,
        verbose=args.verbose,
        system_prompt=system_prompt,
        fallback=args.fallback
    )

    # Output
    if args.output:
        Path(args.output).write_text(
            json.dumps(result, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        print(f"Result saved to: {args.output}")
    elif args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(result.get("content", ""))

        if args.verbose:
            print(f"\n[Turns: {result['turns']}, Tools called: {len(result['tools_called'])}]")


if __name__ == "__main__":
    main()
