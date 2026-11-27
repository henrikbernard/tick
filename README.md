# tick — Universal Automatic Database Discovery for Data Engineering

`tick` connects to any SQLAlchemy-supported database (**PostgreSQL, SQL Server, DuckDB, SQLite, MySQL/MariaDB, Snowflake, BigQuery, Oracle**) and automatically discovers everything a data engineer needs before building pipelines or dbt models.

## Features

- **Universal Schema Catalog & Stats**: Introspects schemas, base tables, views, materialized views, column types (with canonical cross-dialect normalization), primary keys, foreign keys, unique constraints, indexes, row counts, and storage sizes.
- **Data Profiling & Freshness**: Computes null rates, distinct counts, cardinality classes, min/max, numeric mean/stddev/quantiles (`p25`/`p50`/`p75`), string lengths, top-K frequencies, and timestamp freshness/staleness detection.
- **Heuristic PII Detection**: Flags sensitive columns (`email`, `phone`, `ssn`, `credit_card` with Luhn check, `ip_address`, `iban`, `secret_token`, `date_of_birth`, `street_address`, `person_name`, `postal_code`) combining column-name patterns and sampled value regexes, with optional `--redact-pii` masking.
- **Relationship & Lineage Discovery**:
  - Collects explicit foreign keys and **infers missing/unenforced joins** using naming conventions + value-overlap verification.
  - Parses view & materialized view SQL via `sqlglot` to build table-to-view and multi-hop view-to-view lineage graphs.
- **Data Engineering Readiness**:
  - Classifies entity roles (`fact`, `dimension`, `event_log`, `bridge`, `reference`, `staging`, `view`).
  - Recommends ingestion strategies (`incremental_merge`, `incremental_append`, `full_refresh`, `cdc_or_snapshot`), watermark columns, partitioning keys, clustering keys, and candidate primary keys.
  - Audits quality issues (missing PKs, empty tables, all-null/high-null columns, constant columns, stale data, unindexed FKs, PII alerts) and assigns a 0–100 health score.
- **Multi-Format Exporters**:
  - **Standalone Interactive HTML Report** (offline-capable single file with searchable catalog, profiling bars, draggable SVG ERD, lineage DAG, quality/PII filter, and copyable dbt YAML).
  - **Markdown Data Dictionary** with Mermaid `erDiagram` & `flowchart` lineage diagrams.
  - **dbt Scaffolding**: Ready-to-use `sources.yml` (with freshness, data tests, relationship tests, and PII metadata) plus `staging/stg_<schema>__<table>.sql` models.
  - **Versioned JSON Snapshots & CI Diffing** (`tick diff before.json after.json --fail-on-breaking`).

## Installation

```bash
pip install -e .

# Install optional database drivers as needed:
pip install -e ".[duckdb]"
pip install -e ".[postgres]"
pip install -e ".[mssql]"
pip install -e ".[mysql]"
pip install -e ".[snowflake]"
pip install -e ".[bigquery]"
pip install -e ".[oracle]"
```

## Quick Start

```bash
# 1. Check available database drivers
tick drivers

# 2. Generate a realistic demo DuckDB (or SQLite) database
tick demo ./demo_warehouse.duckdb

# 3. Run full automatic discovery and generate all outputs
tick discover ./demo_warehouse.duckdb --out-dir ./discovery-output

# 4. Point at any database URL
tick discover "postgresql://user:pass@localhost:5432/analytics" -o ./pg-discovery
tick discover "mssql+pyodbc://user:pass@host:1433/appdb?driver=ODBC+Driver+18+for+SQL+Server" -o ./mssql-discovery
tick discover ./local.sqlite --redact-pii --html ./report.html --dbt-dir ./dbt_models

# 5. Diff two snapshots in CI to catch breaking schema changes or metric drift
tick diff before.json after.json --md drift.md --fail-on-breaking
```

## Python API

```python
from tick import DiscoveryConfig, discover_database, diff_snapshots

result = discover_database(
    "duckdb:///demo_warehouse.duckdb",
    config=DiscoveryConfig(profile_mode="full", redact_pii=True),
)
print(result.overview.model_dump())
```
