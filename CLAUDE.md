# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Migrate 3 enterprise systems from ELK (Elasticsearch + Kibana) and PostgreSQL to a data lake:
- **FMS** — 61 nodes (33 dashboards, 28 indices/transforms), sources: MongoDB CDC, MySQL CDC
- **POSe** — 79 nodes (21 dashboards), sources: PostgreSQL CDC, MongoDB CDC
- **TAP** — 46 nodes (9 dashboards), sources: PostgreSQL CDC, MySQL CDC (picas.* 15 tables + mavis.* 4 tables)

Total: 186 nodes, 254 edges, 63 dashboards. Each CSV row is a Dashboard or Index with a `Depend On` column referencing other rows by Number.

## Key Files

- `COMPREHENSIVE-MIGRATION-PLAN.md` — unified 3-system migration plan with 2-month parallel execution timeline
- `analyze_dependencies.py` — FMS dependency analyzer (NetworkX graph, priority ranking, PNG + Mermaid output)
- `Migrations Plan - ELK FMS.csv` — FMS source dependency data (61 rows)
- `FMS/FMS-migration-plan.md` — FMS medallion architecture details (Bronze → Silver → Gold)
- `FMS/Thermal/thermal_prediction_pipeline.py` — Prophet-based thermal paper prediction pipeline (checkpointed, 7 stages)
- `FMS/Timeout/webhook_monitoring_timeout-001.py` — ELK timeout monitoring webhook (Google Chat)
- `FMS/Timeout/webhook_monitoring_timeout_starrocks.py` — StarRocks version of the timeout webhook
- `POSE/analyze_dependencies.py` — POSe dependency analyzer
- `POSE/Migrations Plan - ELK & PostgreSQL POSe.csv` — POSe source data (79 nodes)
- `POSE/POSE-migration-plan.md` — POSe migration plan
- `TAP/analyze_dependencies.py` — TAP dependency analyzer
- `TAP/Migrations Plan - ELK & PostgreSQL PCSDatahub.csv` — TAP source data (46 nodes)
- `TAP/TAP-migration-plan.md` — TAP migration plan
- `*_dependency_graph.png` / `*_dependency_graph.md` — generated outputs (re-run scripts to regenerate)

## Commands

```powershell
# FMS dependency analysis (regenerates graph PNG + Mermaid MD + prints priority table)
.\.venv\Scripts\python.exe analyze_dependencies.py

# POSe dependency analysis
.\.venv\Scripts\python.exe POSE\analyze_dependencies.py

# TAP dependency analysis
.\.venv\Scripts\python.exe TAP\analyze_dependencies.py

# Thermal paper prediction pipeline (auto-resumes from last checkpoint)
.\.venv\Scripts\python.exe FMS\Thermal\thermal_prediction_pipeline.py --year 2026 --month 5

# Force full restart (removes manifest checkpoint)
.\.venv\Scripts\python.exe FMS\Thermal\thermal_prediction_pipeline.py --reset

# Install/update dependencies
.\.venv\Scripts\python.exe -m pip install networkx matplotlib
```

The Python environment uses `uv` — `.venv/` is a uv-managed venv. Always use the venv Python directly
(`.\.venv\Scripts\python.exe`) rather than `uv run` which may not find installed packages on Windows.

**Dependencies by script**:
- `analyze_dependencies.py` (all 3 variants): `networkx`, `matplotlib`
- `thermal_prediction_pipeline.py`: `prophet`, `pymysql`, `pandas`, `numpy`, `hijri_converter`, `python-dateutil`
- `webhook_monitoring_timeout-001.py`: `elasticsearch`, `pandas`, `numpy`, `requests`, `pytz`
- `webhook_monitoring_timeout_starrocks.py`: `pymysql`, `pandas`, `numpy`, `requests`, `pytz`

## CSV Structure

The `Depend On` column uses bracket notation like `[4,5,6,7]` referencing row `Number` values.
Edge direction in the graph: provider → dependent (row 6 → row 3 means row 3 depends on row 6).
Children count = out-degree = how many entities depend on a given node. Higher = higher migration priority.

## FMS Migration Priority (by children count)

Top 6 nodes to migrate first:
1. `logstash-transaction` (ID 6) — 14 dependents
2. `logstash-population_with_hb_trx_logon_ticket` (ID 4) — 13 dependents
3. `logstash-ticket_maintenance-v02` (ID 9) — 7 dependents
4. `logstash-logon` (ID 26) — 6 dependents
5. `logstash-ticket_implementation-v02` (ID 12) — 6 dependents
6. `logstash-hb_primary` (ID 39) — 5 dependents

All 33 FMS dashboards are leaf nodes (0 dependents) — they migrate last. Run each system's `analyze_dependencies.py` to see its full priority table.

## Target Architecture

All 3 systems share the same target stack with a **Medallion** (Bronze → Silver → Gold) pattern:

- **Storage**: Object storage with Apache Iceberg table format
- **Catalog**: Apache Polaris (namespace management, RBAC)
- **Ingestion**: Airbyte CDC connectors (PostgreSQL, MySQL, MongoDB sources)
- **Query Engine**: StarRocks (SQL analytics, materialized views)
- **Dashboards**: Apache Superset (replacing Kibana)
- **Bronze** — raw data as Iceberg tables (no transformation), Polaris namespace `bronze.*`
- **Silver** — deduped/enriched views or materialized views (SCD Type 2), namespace `silver.*`
- **Gold** — pre-aggregated materialized views for dashboard queries, namespace `gold.*`

The migration plan optimizes ELK transform chains: single-consumer transforms become SQL views, multi-step enrichment chains collapse into one materialized view, and runtime field scripts become query-time SQL expressions. See `COMPREHENSIVE-MIGRATION-PLAN.md` for full details.

## Thermal Prediction Pipeline

`FMS/Thermal/thermal_prediction_pipeline.py` replaces 6 separate scripts with a single unified pipeline. It uses Prophet to forecast thermal paper consumption per MID (merchant terminal).

**Usage**: Pass `--year` and `--month` to predict a full month. Example: `--year 2026 --month 5` trains on May 2024–April 2026, predicts all of May 2026. Default training window is 2 years (`--train-years`).

**Pipeline stages** (each is checkpointed; re-run skips completed stages):
1. `reference_data` — fetch population, MIDs, behavior, global trends from StarRocks
2. `predict_transaction` — Prophet forecast per MID, batched (1000 MIDs/batch, resume on failure)
3. `predict_settlement` — same for settlement count
4. `predict_generate_qr` — same for generate QR count
5. `transform` — merge 3 predictions + behavior → paper roll counts (cm → rolls)
6. `adjust` — apply roll adjustments from ticket maintenance data
7. `output` — write final predictions to StarRocks `gold.fms.thermal_paper_prediction`

**Idempotency**: A `manifest.json` in the run directory tracks completed stages and per-param batch numbers. Re-running the script resumes from the last successful batch. Use `--reset` to start fresh.
