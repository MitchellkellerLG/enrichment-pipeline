# B2B Company Enrichment Pipeline

A multi-agent pipeline for enriching B2B company data with ICP qualification, hiring signals, and news intelligence.

## Overview

This pipeline uses AI agents to research companies and extract structured data:

| Module | Purpose | Output |
|--------|---------|--------|
| **ICP Qualification** | Determine if company is B2B, what they do, who they serve | `is_b2b`, `three_sentence_summary`, `services_list` |
| **Careers & LinkedIn** | Find careers page, hiring status, open roles | `hiring_intensity`, `all_job_titles`, `linkedin_url` |
| **News Intelligence** | Recent funding, partnerships, announcements | `has_funding_news`, `outreach_timing`, `recent_announcements` |

## Performance

- **Accuracy**: 96%+ usable output rate (with retry logic)
- **Cost**: ~$0.005 per company
- **Speed**: ~30 companies/minute with parallel processing

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Copy .env.example to .env and add your API keys
cp .env.example .env

# Run on test companies
python -m src.pipeline.anneal_harness --sample 5

# Run with parallelism
python -m src.pipeline.anneal_harness --parallel 10 --file companies.csv
```

## Project Structure

```
enrichment-pipeline/
├── src/
│   ├── tools/              # API integrations
│   │   ├── spider/         # Spider Cloud scraper (JS rendering)
│   │   ├── firecrawl/      # Anti-bot resistant scraping
│   │   ├── call_llm_openrouter.py  # LLM with retry/fallback
│   │   └── serper_search.py        # Web search
│   ├── prompts/            # Versioned agent prompts
│   │   ├── icp_qualification.md
│   │   ├── careers_linkedin.md
│   │   └── news_intelligence.md
│   ├── pipeline/           # Orchestration
│   │   ├── runners.py      # Individual module runners
│   │   ├── anneal_harness.py  # Testing harness
│   │   └── orchestrator.py    # Full pipeline
│   └── schemas/            # JSON schemas for validation
└── requirements.txt
```

## API Keys Required

| Service | Purpose | Get Key |
|---------|---------|---------|
| OpenRouter | LLM API | https://openrouter.ai |
| Spider Cloud | Web scraping | https://spider.cloud |
| Serper | Google search | https://serper.dev |
| Firecrawl (optional) | Anti-bot scraping | https://firecrawl.dev |

## Usage

### Single Company

```python
from src.pipeline.runners import run_icp_qual, run_careers_linkedin, run_news_intel

company = "Traba"
website = "https://traba.work"

icp = run_icp_qual(company, website)
careers = run_careers_linkedin(company, website)
news = run_news_intel(company, website)
```

### Batch Processing (Testing)

```bash
# Run anneal test on 25 companies (validates accuracy before production)
python -m src.pipeline.anneal_harness --sample 25 --parallel 10

# Run from CSV file with batch webhook push
python -m src.pipeline.anneal_harness --file companies.csv --parallel 30 --push-webhook
```

### Production Run (Streaming Webhook)

For production, use `production_run.py` which pushes each company to the webhook **immediately** after completion (1 record per webhook call):

```bash
# Test with 50 company sample first (recommended)
python -m src.pipeline.production_run --file companies.csv --sample 50 --parallel 10

# Dry run (no webhook push, just see results)
python -m src.pipeline.production_run --file companies.csv --sample 50 --dry-run

# Full production run
python -m src.pipeline.production_run --file companies.csv --parallel 30

# Save results to JSON
python -m src.pipeline.production_run --file companies.csv --output results.json
```

### CLI Options

**anneal_harness.py** (testing/validation):

```
--sample N       Number of test companies (default: 9)
--file FILE      CSV with company_name,website columns
--parallel N     Concurrent companies (default: 1, max recommended: 30)
--push-webhook   Push ALL results to webhook at end
--force-push     Push even if accuracy gate fails
```

**production_run.py** (production with streaming):

```
--file FILE      CSV with company_name,website columns (required)
--sample N       Only process first N companies (for testing)
--parallel N     Concurrent workers (default: 10, max: 30)
--webhook URL    Override webhook URL from env
--dry-run        Skip webhook pushes
--output FILE    Save results to JSON
```

## Prompt Versioning

Prompts are stored as markdown files with version history and learned constraints:

```markdown
# Careers & LinkedIn Prompt v3.1

## Version History
- v3.1 (2026-01-11): Raw Data First - exact job titles with audit trail

## Prompt
...

## Known Failure Modes
1. **"Loading roles..." returned** - JS-rendered careers pages
   - Fix: Added wait_for_js to spider config
```

## Architecture

```
Input (company_name, website)
         │
         ▼
┌─────────────────┐
│ ICP Qualification│ ─── scrape_url, web_search
└────────┬────────┘
         │ DISQUALIFIED? → STOP
         ▼
┌─────────────────┐
│ Careers/LinkedIn│ ─── web_search, scrape_url, firecrawl
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ News Intelligence│ ─── web_search (2 searches)
└────────┬────────┘
         │
         ▼
    JSON Output → Clay Webhook
```

## License

MIT
