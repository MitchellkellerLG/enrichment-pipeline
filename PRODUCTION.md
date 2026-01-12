# Production Operations Guide

Operational runbook for running the enrichment pipeline in production.

---

## Prerequisites

- **Python 3.11+** (virtual environment recommended)
- **API Keys** configured in `.env`:

| Key | Required | Purpose |
|-----|----------|---------|
| `OPENROUTER_API_KEY` | Yes | LLM inference for ICP classification |
| `SPIDER_API_KEY` | Yes | Web scraping (careers, company pages) |
| `SERPER_API_KEY` | Yes | News search |
| `CLAY_WEBHOOK_URL` | Optional | Destination for enriched data |

Copy `.env.example` to `.env` and populate all required keys before running.

---

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Test Run (50 companies)

```bash
python -m src.pipeline.production_run --file companies.csv --sample 50 --dry-run
```

This processes 50 companies without pushing webhooks. Use this to verify:
- CSV parsing works correctly
- API keys are valid
- Pipeline flow executes without errors

### 3. Production Run

```bash
python -m src.pipeline.production_run --file companies.csv --parallel 30
```

Full production execution with 30 concurrent workers.

### 4. Resume After Interruption

```bash
python -m src.pipeline.production_run --file companies.csv --resume <run_id>
```

The `run_id` is printed at the start of each run and stored in the state database.

---

## DAG Flow

The pipeline executes as a directed acyclic graph with conditional gates:

```
                         +---> [careers] ---+
                         |                  |
mx_check --> (mx_pass?) --> icp_check --> (icp_pass?) --+--> webhook_push
                         |                  |
                         +---> [news] ------+
```

### Gate Behavior

| Gate | Result | Action |
|------|--------|--------|
| `mx_pass = false` | Bad email domain | Skip all nodes, push minimal webhook with `DISQUALIFIED` |
| `mx_pass = true` | Valid B2B email | Continue to ICP check |
| `icp_pass = false` | Not ICP fit | Skip careers/news, push with `DISQUALIFIED` tier |
| `icp_pass = true` | ICP match | Run careers + news in parallel, then push |

### Node Descriptions

| Node | Purpose | Data Source |
|------|---------|-------------|
| `mx_check` | Validate email domain has valid MX records | DNS lookup |
| `icp_check` | Classify company against ICP criteria | Spider scrape + LLM |
| `careers` | Extract job postings for hiring signals | Spider scrape |
| `news` | Find recent company news/press | Serper search |
| `webhook_push` | Send enriched data to Clay | HTTP POST |

---

## Cost Breakdown

Estimated per-company costs based on which nodes execute:

| Scenario | Nodes Run | Est. Cost |
|----------|-----------|-----------|
| Qualified B2B | All 5 | ~$0.005 |
| Disqualified (bad MX) | 1 | ~$0.0001 |
| Disqualified (not ICP) | 2 | ~$0.002 |

### Cost Components

| Provider | Operation | Cost |
|----------|-----------|------|
| OpenRouter | ICP classification | ~$0.001/call |
| Spider | Page scrape | ~$0.001/page |
| Serper | News search | ~$0.001/query |

**Example**: 10,000 companies where 85% pass MX and 70% pass ICP:
- MX failures: 1,500 x $0.0001 = $0.15
- ICP failures: 2,125 x $0.002 = $4.25
- Qualified: 5,950 x $0.005 = $29.75
- **Total: ~$34.15**

---

## Cache Strategy

The pipeline caches API responses to reduce costs and improve resume speed:

| Artifact | TTL | Rationale |
|----------|-----|-----------|
| MX check | 30 days | DNS records rarely change |
| ICP scrape | 14 days | Company descriptions stable |
| Careers scrape | 3 days | Job postings change weekly |
| News search | 12 hours | News is time-sensitive |
| LinkedIn URL | 90 days | Company LinkedIn URLs are stable |

### Cache Behavior

- **Cache hit**: Skip API call, use stored result
- **Cache miss**: Make API call, store result with TTL
- **Expired**: Treat as miss, refresh data

Re-running the same companies within cache TTL costs near-zero.

---

## State Files

State is persisted to SQLite for resumability:

```
.state/
├── pipeline.db    # SQLite state machine (node-level state)
└── cache.db       # Cache with TTL
```

### Inspecting State

View company processing status:
```bash
sqlite3 .state/pipeline.db "SELECT * FROM company_state WHERE run_id='<run_id>' LIMIT 10;"
```

View run summary:
```bash
sqlite3 .state/pipeline.db "SELECT status, COUNT(*) FROM company_state WHERE run_id='<run_id>' GROUP BY status;"
```

Check cache entries:
```bash
sqlite3 .state/cache.db "SELECT key, expires_at FROM cache ORDER BY expires_at DESC LIMIT 10;"
```

---

## Monitoring

### Progress Output

During execution, the pipeline prints progress updates:

```
[150/1000] Acme Corp: mx:OK icp:OK careers:OK news:OK  (52.3/min, ETA: 16.2min)
```

Status codes:
- `OK` - Node completed successfully
- `SKIP` - Node skipped (gate failed)
- `FAIL` - Node failed (will retry)
- `CACHE` - Result from cache

### Stats at End

Final summary printed after completion:

```
=== Run Complete ===
Total: 1000 companies
MX Passed: 950 (95%)
ICP Passed: 800 (84%)
Webhooks Pushed: 800
Total Cost: $4.12
Duration: 19.2 minutes
Rate: 52.1 companies/min
```

---

## Error Handling

| Error | Symptom | Action |
|-------|---------|--------|
| API rate limit | 429 responses | Reduce `--parallel`, wait 60s, resume |
| Webhook timeout | Hanging on push | Check Clay URL, verify endpoint, retry failed |
| LLM parse failure | JSON decode errors | Automatic retry (2 attempts), then skip |
| Process killed | Ctrl+C or OOM | Resume with `--resume <run_id>` |
| Network timeout | Connection errors | Automatic retry with backoff |
| Invalid CSV | "No companies" error | Check CSV has `company_name` and `website` columns |

### Automatic Retry

The pipeline automatically retries transient failures:
- Network timeouts: 3 attempts with exponential backoff
- LLM parse failures: 2 attempts with different prompt
- Webhook failures: 2 attempts with 30s delay

---

## CLI Reference

```
Usage: python -m src.pipeline.production_run [OPTIONS]

Options:
  --file FILE        CSV with company_name,website columns (required)
  --sample N         Only process first N companies
  --parallel N       Concurrent workers (default: 10, max: 50)
  --webhook URL      Override webhook URL from env
  --dry-run          Skip webhook pushes
  --resume RUN_ID    Resume from previous run
  --verbose          Print detailed output per node
  --output FILE      Save results to JSON file
  --help             Show this message and exit
```

### Common Patterns

```bash
# Validate setup without processing
python -m src.pipeline.production_run --file companies.csv --sample 1 --dry-run --verbose

# Process subset for testing
python -m src.pipeline.production_run --file companies.csv --sample 100 --parallel 5

# Full production with output file
python -m src.pipeline.production_run --file companies.csv --parallel 30 --output results.json

# Resume interrupted run
python -m src.pipeline.production_run --file companies.csv --resume 2026-01-12_143022
```

---

## Stress Testing (Future)

Find rate limit knee points for each provider:

```bash
python -m src.pipeline.production_run --stress 100 --duration 5m --provider openrouter
```

This will:
1. Ramp up to 100 concurrent requests
2. Measure throughput and error rate
3. Report the sustainable parallelism level

---

## Troubleshooting

### "No companies to process"

1. Check CSV has correct headers:
   ```bash
   head -1 companies.csv
   # Expected: company_name,website,...
   ```
2. Check `--sample` is not set to 0
3. Verify CSV is not empty

### "Webhook failed"

1. Verify `CLAY_WEBHOOK_URL` is set:
   ```bash
   grep CLAY_WEBHOOK_URL .env
   ```
2. Test webhook manually:
   ```bash
   curl -X POST $CLAY_WEBHOOK_URL -H "Content-Type: application/json" -d '{"test": true}'
   ```
3. Check Clay table is accepting data

### "MX check failing for all"

1. Install DNS library if missing:
   ```bash
   pip install dnspython
   ```
2. Test DNS resolution:
   ```bash
   python -c "import dns.resolver; print(dns.resolver.resolve('google.com', 'MX'))"
   ```
3. Check network/firewall allows DNS queries

### "LLM returning invalid JSON"

1. Check OpenRouter API key is valid
2. Verify model is available (check OpenRouter status page)
3. Try with `--verbose` to see raw LLM responses
4. May need to adjust prompt in `src/nodes/icp_check.py`

### "Slow processing speed"

1. Increase parallelism: `--parallel 30` (up to 50)
2. Check for cache hits (should be fast)
3. Monitor rate limit errors (may need to reduce parallelism)
4. Check network latency to API providers

---

## Best Practices

### 1. Always Test First

Before any production run:
```bash
python -m src.pipeline.production_run --file companies.csv --sample 50 --dry-run
```

This validates the entire flow without side effects.

### 2. Monitor Costs

Check running totals in state DB:
```bash
sqlite3 .state/pipeline.db "SELECT SUM(cost) FROM company_state WHERE run_id='<run_id>';"
```

Set up alerts if costs exceed expectations.

### 3. Use Cache

Re-runs of the same companies use cached data automatically. Benefits:
- Near-zero cost for cached nodes
- Faster processing
- Resume from cache after any failure

### 4. Resume, Don't Restart

After any interruption:
```bash
# DO THIS
python -m src.pipeline.production_run --file companies.csv --resume <run_id>

# NOT THIS (wastes API calls, duplicates data)
python -m src.pipeline.production_run --file companies.csv
```

### 5. Scale Gradually

Start with lower parallelism and increase:
```bash
# Start conservative
--parallel 10

# If no rate limits after 100 companies, increase
--parallel 20

# Maximum stable for most providers
--parallel 30
```

### 6. Keep State Files

Do not delete `.state/` directory between runs:
- Enables resume capability
- Preserves cache for cost savings
- Maintains run history

---

## File Structure Reference

```
enrichment-pipeline/
├── .env                 # API keys (never commit)
├── .env.example         # Template for .env
├── .state/              # State and cache databases
│   ├── pipeline.db      # Run state
│   └── cache.db         # API response cache
├── requirements.txt     # Python dependencies
├── src/
│   ├── pipeline/
│   │   └── production_run.py  # Main entry point
│   ├── nodes/           # Individual DAG nodes
│   ├── gates/           # Conditional logic
│   └── utils/           # Shared utilities
└── PRODUCTION.md        # This file
```
