# 📊 Success Example — Operational Report Automation

## The problem (before)

Every morning, the data team spent **~4 hours** doing the same work across SAS Viya, Python scripts, and Excel:

| Step | Task | Time |
|------|------|------|
| 1 | Download and merge event logs from 3 regions | 40 min |
| 2 | Rename inconsistent column names by hand | 20 min |
| 3 | Fix data types broken on CSV export | 15 min |
| 4 | Filter out error events manually | 10 min |
| 5 | Fill in blank region tags from partial outages | 10 min |
| 6 | Compute latency SLA flags and throughput classes | 30 min |
| 7 | Remove duplicate events from overlapping log windows | 20 min |
| 8 | Copy results into the reporting database | 40 min |
| 9 | Write the daily summary message | 15 min |
| **Total** | | **~4h / day** |

With 22 working days per month → **88 hours/month** burned on pure repetition.

---

## The solution (after)

One config file. One command.

```bash
python run_pipeline.py examples/operational_reports/pipeline.yaml
```

Runtime: **~20 minutes**, zero manual steps, full audit trail.

---

## Impact

| | Before | After |
|---|---|---|
| Time per day | ~4h | ~20 min |
| Manual steps | 8 | 0 |
| Error risk | High (human) | Low (deterministic) |
| Who can run it | 1 person | Entire team |
| Audit trail | None | Full (run_id, rows, timing) |
| Hours per month | 88h | ~6h (monitoring only) |

**Net reduction: 90%** — from 88h to ~6h per month.  
Yearly savings: ~984 hours (~6 months of a work year recovered).

---

## Files

```
operational_reports/
├── pipeline.yaml    # Pipeline config — the "recipe" that replaced 8 manual steps
├── raw_events.csv   # Sample input — replica of distributed event log export
└── README.md        # This file
```

---

## How to schedule (production)

```bash
# Run daily at 06:00 via cron (Linux)
0 6 * * * /usr/bin/python /path/to/run_pipeline.py /path/to/pipeline.yaml

# Check the run history anytime
python run_pipeline.py --history --pipeline operational_report_automation_v1
```

---

## Architecture note

This example simulates the architecture used at **Very Tecnologia**, where analytical pipelines feed predictive models via a distributed processing layer (SAS Viya + Python). In production, the `source` block would point to a DuckDB Data Lake volume or a SAS Viya REST endpoint, and the `destination` block would write to the shared analytical warehouse. The config file is the only thing that changes — the engine stays the same.
