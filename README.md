# tick — Universal Automatic Database Discovery for Data Engineering & Databricks

`tick` connects to any SQLAlchemy-supported database (**PostgreSQL, SQL Server, DuckDB, SQLite, MySQL/MariaDB, Databricks Unity Catalog, Oracle, IBM DB2/AS400, SAP HANA, Snowflake, BigQuery**) and automatically discovers everything a data engineer needs before building pipelines—with first-class **Databricks Lakehouse** and **Manufacturing / ERP / MES / OT** intelligence.

## Capabilities

### Core Discovery & Lineage
- **Universal Schema Catalog & Stats**: Introspects schemas, tables, views, materialized views, canonical cross-dialect types, PKs, FKs, unique constraints, indexes, row counts, and sizes.
- **Data Profiling & Freshness**: Null rates, distinct counts, cardinality classes, min/max, numeric mean/stddev/quantiles (`p25`/`p50`/`p75`), string lengths, top-K frequencies, and timestamp freshness/staleness alerts.
- **Relationship & Lineage Discovery**: Collects explicit FKs, infers missing/unenforced joins with value-overlap verification, and parses view SQL via `sqlglot` for multi-hop lineage graphs.
- **Data Engineering Blueprint**: Classifies entity roles (`fact`, `dimension`, `event_log`, `bridge`, `reference`, `staging`, `view`), recommends ingestion strategies, watermarks, candidate PKs, and 0–100 table health scores.

### Databricks & Lakeflow Readiness
- **Unity Catalog Connector**: Connect directly to Databricks SQL Warehouses (`databricks://...`) alongside upstream OLTP/ERP databases.
- **Lakeflow Connect / CDC Readiness Audit**: Inspects SQL Server Change Tracking/CDC, PostgreSQL `wal_level`/`REPLICA IDENTITY`, Oracle `ARCHIVELOG`/supplemental logging, MySQL `binlog_format`, PK presence, and unsupported column types.
- **Spark / Delta Lake Compatibility Linter**: Flags Delta-illegal column names (`Order #`, `Qty/Hr`), `DECIMAL` precision > 38, `MONEY`/exotic types, timezone-naive plant timestamps (`TIMESTAMP_NTZ`), and over-partitioning vs. **Liquid Clustering (`CLUSTER BY`)**.
- **Databricks Asset Exporter (`--databricks-dir`)**:
  - `unity_catalog_ddl.sql`: Delta `CREATE TABLE` with Liquid Clustering, CDF, Deletion Vectors, comments, Unity Catalog governance tags, and row-filter/column-mask stubs.
  - `lakeflow_pipeline.py`: Ready-to-run **Lakeflow Declarative Pipelines (DLT)** Bronze → Silver code with column sanitization, legacy date parsing, `@dlt.expect_all` data quality rules, `dlt.apply_changes` Auto CDC, and Structured Streaming watermarks.
  - `ingestion_sizing.md`: Backfill size, daily incremental delta rate, cadence, and Databricks compute tier plan.

### Manufacturing, OT & Multi-Plant Intelligence
- **Production Shop-Floor Safety Guardrails**: `--read-uncommitted` (`WITH (NOLOCK)` / `WITH UR`), `--query-timeout`, `--skip-empty-tables` (ERP empty-table triage), `--skip-tables-over-mb`, and `--use-tablesample`.
- **ERP / SAP Semantic Glossary**: Built-in SAP/MES glossary (`MARA`, `AUFK`, `WERKS`, `MATNR`, etc.), auto-extraction from SAP `DD02T`/`DD03T` if present, and custom `--glossary custom.yml` overrides.
- **Legacy Date/Time Encoding Detection**: Detects `YYYYMMDD`, Julian (`CYYDDD`/`YYYYDDD`), Excel serial dates, `HHMMSS`, and split DATE + TIME pairs (`ERDAT` + `ERZET`) with ready-to-use PySpark stitching expressions.
- **OT Historian & PLC Telemetry Detection**: Recognizes narrow EAV tag tables and wide PLC polling tables, measures polling cadence/gaps, catches flatlined sensors and bad OPC quality codes, and recommends streaming watermark delays & Liquid Clustering.
- **Recursive BOM & Lot Genealogy Detection**: Identifies self-referential parent/child part, lot, work-order, and asset hierarchies.
- **Manufacturing Governance**: Classifies plant isolation keys (`plant_id`, `werks` → UC row filters), commercial cost/margin columns (`unit_cost` → UC column masks), and ITAR/CAD/IP fields (`cad_drawing_path` → UC compliance tags) alongside consumer PII.
- **Cross-Plant Schema Harmonization (`tick compare-sources`)**: Compares N plant snapshots to detect site-only columns, PK divergence, and type conflicts, generating unified multi-plant Databricks `UNION ALL` SQL.

## Installation

```bash
pip install -e .

# Optional database drivers:
pip install -e ".[duckdb]"
pip install -e ".[databricks]"
pip install -e ".[postgres]"
pip install -e ".[mssql]"
pip install -e ".[oracle]"
pip install -e ".[mysql]"
pip install -e ".[db2]"
pip install -e ".[hana]"
```

## Quick Start

```bash
# Check installed drivers
tick drivers

# Generate Smart Factory (SAP + MES + OT Historian + BOM) demos for two plants
tick demo ./plant_ohio.duckdb --domain manufacturing --plant plant_ohio
tick demo ./plant_munich.duckdb --domain manufacturing --plant plant_munich

# Discover Plant Ohio with production safety guardrails & full Databricks + dbt + HTML bundle
tick discover ./plant_ohio.duckdb \
  --out-dir ./ohio-discovery \
  --skip-empty-tables \
  --use-tablesample \
  --databricks-catalog manufacturing \
  --databricks-schema bronze_ohio

# Discover Plant Munich snapshot
tick discover ./plant_munich.duckdb --json ./munich_snapshot.json --quiet

# Harmonize schemas across plants and generate unified Databricks SQL
tick compare-sources ./ohio-discovery/snapshot.json ./munich_snapshot.json \
  --md ./harmonization.md \
  --sql ./unified_silver_views.sql

# Detect schema drift in CI between two point-in-time snapshots
tick diff before.json after.json --md drift.md --fail-on-breaking
```
