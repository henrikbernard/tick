# tick — Universal Automatic Database Discovery for Data Engineering & Databricks

[![CI](https://github.com/henrikbernard/tick/actions/workflows/ci.yml/badge.svg)](https://github.com/henrikbernard/tick/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)

`tick` points at **any SQLAlchemy-supported database** (**PostgreSQL, SQL Server, DuckDB, SQLite, MySQL/MariaDB, Databricks Unity Catalog, Oracle, IBM DB2/AS400, SAP HANA, Snowflake, BigQuery**) and automatically discovers everything a data engineer needs before building pipelines—with first-class **Databricks Lakehouse**, **Manufacturing / ERP / MES / OT**, **Data Contracts**, and **standalone offline HTML dashboards**.

```mermaid
flowchart LR
    SRC[("Any Source DB\nPostgres · MSSQL · Oracle · SAP\nDB2 · MySQL · DuckDB · Databricks")] --> CAT["Catalog & Safe Guardrails\n(NOLOCK / Timeout / Empty Triage)"]
    CAT --> PROF["Parallel Profiler (--workers N)\nStats · Quantiles · Freshness · PII · UC Gov"]
    PROF --> INT["DE & Manufacturing Intelligence\nFK Inference · SQL Lineage · CDC Audit\nOT Telemetry · BOMs · Legacy Dates · Delta Lint"]
    INT --> HTML["3 Interactive HTML Dashboards\nDiscovery · Multi-Plant · Diff"]
    INT --> DBX["Databricks Bundle\nUC DDL · Lakeflow DLT · Sizing"]
    INT --> CTR["Data Contracts\nDatabricks DQX · ODCS v3.0"]
    INT --> DBT["dbt & Docs\nsources.yml · Staging SQL · Markdown"]
```

---

## Key Capabilities

### 1. Core Discovery, Profiling & Lineage
- **Universal Schema Catalog & Stats**: Introspects schemas, tables, views, materialized views, cross-dialect canonical types, PKs, FKs, unique constraints, indexes, row counts, and sizes.
- **Parallel Data Profiling (`--workers N`)**: Bounded thread-pool profiling for large ERP databases with null rates, distinct counts, cardinality classes, min/max, numeric mean/stddev/quantiles (`p25`/`p50`/`p75`), string lengths, top-K frequencies, and timestamp freshness/staleness checks.
- **Relationship & View SQL Lineage**: Collects explicit FKs, infers missing/unenforced joins with value-overlap verification, and parses view SQL via `sqlglot` for multi-hop lineage graphs.
- **Data Engineering Blueprint**: Classifies entity roles (`fact`, `dimension`, `event_log`, `bridge`, `reference`, `staging`, `view`), recommends ingestion strategies, watermarks, candidate PKs, and 0–100 table health scores.

### 2. Three Standalone Offline HTML Dashboards *(zero external CDNs)*
- **Discovery Dashboard (`tick discover --html`)**:
  - Overview KPIs, inline SVG **CDC Readiness Donut**, Health & Entity Role charts, and a 3-tier **Databricks Migration Wave Planner** (Wave 1 Fast-Track / Wave 2 Needs Config / Wave 3 Remediation Required) with Bronze MB/GB & daily row rollups.
  - Interactive **Catalog & Profiler** with column search/PII/null filters, inline SVG **Numeric Quantile Boxplots**, and proportional **Top-Value Bars**.
  - **Interactive ERD & Lineage DAG** with drag-to-pan, scroll-wheel zoom, live highlight search, Fit View, cycle handling, and one-click **Download SVG**.
  - Persisted Light/Dark theme toggle, **Print / PDF** executive summary layout, and downloadable Unity Catalog DDL / Lakeflow DLT / dbt code.
- **Multi-Plant Harmonization Dashboard (`tick compare-sources --html`)**:
  - Interactive **Plant × Table Presence & Drift Heatmap**, alignment donut, drift breakdown bars, click-to-inspect column conflict & cast matrices, and copy/download **Unified Multi-Plant Databricks `UNION ALL` SQL**.
- **Schema & Data Drift Dashboard (`tick diff --html`)**:
  - Breaking vs Safe CI gate banner, added/removed tables & PK change lists, filterable column diffs (including breaking type narrowings), newly detected PII alerts, and Before vs After metric drift bars.

### 3. Databricks Lakeflow, Unity Catalog & Data Contracts
- **Unity Catalog & Lakeflow Connect CDC Audit**: Inspects SQL Server Change Tracking/CDC, PostgreSQL `wal_level`/`REPLICA IDENTITY`, Oracle `ARCHIVELOG`/supplemental logging, MySQL `binlog_format`, PK presence, and unsupported column types.
- **Spark / Delta Lake Compatibility Linter**: Flags Delta-illegal column names (`Order #`, `Qty/Hr`), `DECIMAL` precision > 38, `MONEY`/exotic types, timezone-naive timestamps (`TIMESTAMP_NTZ`), and over-partitioning vs. **Liquid Clustering (`CLUSTER BY`)**.
- **Databricks Asset Bundle (`--databricks-dir`)**:
  - `unity_catalog_ddl.sql`: Delta `CREATE TABLE` with Liquid Clustering, CDF, Deletion Vectors, comments, Unity Catalog governance tags, and type-matched row-filter / column-mask templates.
  - `lakeflow_pipeline.py`: Ready-to-run **Lakeflow Declarative Pipelines (DLT)** Bronze → Silver code with column sanitization, legacy date stitching, `@dlt.expect_all` rules, `dlt.apply_changes` Auto CDC, batch Materialized Views for `full_refresh`, and Structured Streaming watermarks.
  - `ingestion_sizing.md`: Backfill size, daily incremental delta rate, cadence, and Databricks compute tier plan.
- **Data Contract Exporter (`--contracts-dir`)**:
  - `dqx_checks.yml`: **Databricks Labs DQX** quality checks (`is_unique`, `is_not_null`, `is_in_list`, `is_in_range`, OT quality `is_not_in_list`) with sensitive PII/governance values excluded.
  - `data_contract.odcs.yml`: **Open Data Contract Standard (ODCS v3.0.0)** YAML with logical/physical Spark types, primary keys, semantic business names, PII/ITAR/commercial classifications, Liquid Clustering metadata, quality rules, and freshness SLAs.

### 4. Manufacturing, ERP & OT Intelligence
- **Production Shop-Floor Safety Guardrails**: `--read-uncommitted` (`WITH (NOLOCK)` / `WITH UR`), `--query-timeout`, `--skip-empty-tables`, `--skip-tables-over-mb`, `--use-tablesample`, and credential masking in URLs/ODBC strings.
- **ERP / SAP Semantic Glossary**: Built-in SAP/MES glossary (`MARA`, `AUFK`, `WERKS`, `MATNR`, `STPRS`, etc.), automatic SAP `DD02T`/`DD03T` extraction, and custom `--glossary custom.yml` overrides.
- **Legacy Date/Time Detection**: `YYYYMMDD`, JDE/SAP Julian (`CYYDDD`/`YYYYDDD`), Excel serial dates, `HHMMSS`, and split DATE + TIME pairs (`ERDAT` + `ERZET`) with PySpark stitching expressions.
- **OT Historian & PLC Telemetry**: Narrow EAV tag tables & wide PLC polling tables, per-tag polling cadence, flatlined sensor detection, OPC Classic/UA bad quality code rates, and watermark delay recommendations.
- **Recursive BOM & Lot Genealogy**: Detects self-referential parent/child part, lot, work-order, and asset trees.
- **Manufacturing Governance**: Plant isolation keys (`plant_id`, `WERKS` → UC row filters), commercial pricing/cost (`unit_cost`, `STPRS` → UC column masks), and ITAR/CAD/IP (`cad_drawing_path` → UC compliance tags).

---

## Installation

```bash
git clone https://github.com/henrikbernard/tick.git
cd tick
pip install -e .

# Optional database drivers:
pip install -e ".[duckdb]"      # DuckDB
pip install -e ".[databricks]"  # Databricks Unity Catalog SQL Warehouse
pip install -e ".[postgres]"    # PostgreSQL (psycopg3)
pip install -e ".[mssql]"       # SQL Server (pyodbc)
pip install -e ".[oracle]"      # Oracle (oracledb)
pip install -e ".[mysql]"       # MySQL / MariaDB (pymysql)
pip install -e ".[db2]"         # IBM DB2 / AS400
pip install -e ".[hana]"        # SAP HANA
pip install -e ".[snowflake]"   # Snowflake
pip install -e ".[bigquery]"    # BigQuery
```

---

## Quick Start

```bash
# 1. Check installed SQLAlchemy database drivers
tick drivers

# 2. Generate Smart Factory (SAP + MES + OT Historian + BOM) demos for two plants
tick demo ./plant_ohio.duckdb --domain manufacturing --plant plant_ohio
tick demo ./plant_munich.duckdb --domain manufacturing --plant plant_munich

# 3. Discover Plant Ohio with parallel workers, safety guardrails, and full artifact bundle
#    Writes: report.html, report.md, snapshot.json, dbt/, databricks/, contracts/
tick discover ./plant_ohio.duckdb \
  --out-dir ./ohio-discovery \
  --workers 4 \
  --skip-empty-tables \
  --use-tablesample \
  --databricks-catalog manufacturing \
  --databricks-schema bronze_ohio

# 4. Discover Plant Munich
tick discover ./plant_munich.duckdb --out-dir ./munich-discovery --workers 4 --quiet

# 5. Harmonize schemas across plants & generate interactive HTML dashboard + unified Databricks SQL
tick compare-sources ./ohio-discovery/snapshot.json ./munich-discovery/snapshot.json \
  --html ./harmonization.html \
  --md ./harmonization.md \
  --sql ./unified_silver_views.sql

# 6. Compare snapshots for CI schema drift & breaking change gating
tick diff ./ohio-discovery/snapshot.json ./munich-discovery/snapshot.json \
  --html ./drift.html \
  --md ./drift.md \
  --fail-on-breaking
```

---

## Output Bundle (`tick discover -o <dir>`)

| Artifact | Purpose |
|---|---|
| `report.html` | Standalone interactive Discovery Dashboard (Overview charts, Wave Planner, Catalog, ERD, Lineage, Quality, Code) |
| `report.md` | GitHub-flavored Markdown data dictionary with Mermaid ERD & Lineage diagrams |
| `snapshot.json` | Machine-readable Pydantic snapshot for `tick diff` and `tick compare-sources` |
| `databricks/unity_catalog_ddl.sql` | Unity Catalog Delta DDL with Liquid Clustering, CDF, tags, and type-matched mask/row-filter stubs |
| `databricks/lakeflow_pipeline.py` | Databricks Lakeflow Declarative Pipeline (DLT) Bronze → Silver Python code |
| `databricks/ingestion_sizing.md` | Backfill volume, daily row velocity, cadence, CDC audit, and Delta linter report |
| `contracts/dqx_checks.yml` | Databricks Labs DQX data quality rules YAML |
| `contracts/data_contract.odcs.yml` | Open Data Contract Standard (ODCS v3.0.0) YAML contract |
| `dbt/sources.yml` + `dbt/staging/*.sql` | dbt v2 sources with freshness/tests and clean staging SQL models |

---

## Development & Testing

```bash
pip install -e ".[dev]"
pytest -v
```
