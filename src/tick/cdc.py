"""Lakeflow Connect & Change Data Capture (CDC) readiness auditor."""

from __future__ import annotations

from sqlalchemy.engine import Engine

from tick.dialects import DialectAdapter, TableCdcProbe
from tick.models import CdcDatabaseAudit, CdcReadiness, DiscoveryConfig, TableInfo

MSSQL_CDC_UNSUPPORTED = {"SQL_VARIANT", "HIERARCHYID", "GEOGRAPHY", "GEOMETRY", "XML", "ROWVERSION", "IMAGE"}
ORACLE_CDC_WATCHLIST = {"LONG", "LONG RAW", "BFILE", "XMLTYPE", "SDO_GEOMETRY"}
POSTGRES_CDC_WATCHLIST = {"XML", "USER-DEFINED", "TSVECTOR", "GEOMETRY"}


def audit_cdc_readiness(
    engine: Engine,
    adapter: DialectAdapter,
    tables: list[TableInfo],
    config: DiscoveryConfig | None = None,
) -> CdcDatabaseAudit:
    """Inspect database & table CDC / Lakeflow Connect readiness across dialects."""
    table_probes: dict[tuple[str, str], TableCdcProbe] = {}

    with engine.connect() as conn:
        if config is not None:
            adapter.configure_safe_session(
                conn,
                read_uncommitted=config.read_uncommitted,
                query_timeout_seconds=config.query_timeout_seconds,
            )
        db_audit = adapter.audit_database_cdc(conn)
        schemas = sorted({t.schema_name for t in tables if t.table_type == "table"})
        for schema in schemas:
            for tbl_name, probe in adapter.audit_table_cdc(conn, schema).items():
                table_probes[(schema, tbl_name)] = probe

    for tbl in tables:
        if tbl.table_type != "table":
            tbl.cdc_readiness = CdcReadiness(
                status="not_applicable",
                connector_recommendation="downstream_lakeflow_view",
                notes=["Views are rebuilt as Silver/Gold Lakeflow Declarative Pipeline tables rather than CDC-replicated."],
            )
            continue

        probe = table_probes.get((tbl.schema_name, tbl.name))
        has_pk = len(tbl.primary_key) > 0
        blockers: list[str] = []
        warnings: list[str] = []
        notes: list[str] = []

        # Dialect-specific unsupported / tricky column types
        for col in tbl.columns:
            raw_u = col.raw_type.upper().split("(")[0].strip()
            if adapter.name == "mssql" and raw_u in MSSQL_CDC_UNSUPPORTED:
                blockers.append(
                    f"Column '{col.name}' has SQL Server type '{col.raw_type}' which is unsupported or requires casting in Lakeflow Connect."
                )
            elif adapter.name == "oracle" and raw_u in ORACLE_CDC_WATCHLIST:
                warnings.append(
                    f"Column '{col.name}' has Oracle type '{col.raw_type}' — verify supplemental logging / LOB handling."
                )
            elif adapter.name == "postgresql" and raw_u in POSTGRES_CDC_WATCHLIST:
                warnings.append(
                    f"Column '{col.name}' has Postgres type '{col.raw_type}' — verify logical replication serialization."
                )

        # Evaluate by dialect
        if adapter.name == "mssql":
            ct_on = probe.change_tracking_enabled if probe else None
            cdc_on = probe.cdc_enabled if probe else None
            if not has_pk:
                warnings.append(
                    "No primary key: SQL Server Change Tracking (preferred for Lakeflow Connect) requires a PK; fall back to SQL Server CDC or snapshot."
                )
            if db_audit.db_change_tracking_enabled is False and db_audit.db_cdc_enabled is False:
                blockers.append("Neither SQL Server Change Tracking nor CDC is enabled at the database level.")
            elif ct_on is False and cdc_on is False:
                warnings.append("Table does not yet have Change Tracking or CDC enabled.")

            connector = "lakeflow_connect_mssql_ct" if has_pk else "lakeflow_connect_mssql_cdc"
            if blockers:
                status = "blocked"
            elif (has_pk and ct_on) or cdc_on:
                status = "ready"
            else:
                status = "needs_config"

        elif adapter.name == "postgresql":
            ri = probe.replica_identity if probe else None
            if db_audit.wal_level and db_audit.wal_level != "logical":
                blockers.append(f"PostgreSQL wal_level is '{db_audit.wal_level}'; must be 'logical' for CDC.")
            if not has_pk and ri != "full":
                blockers.append("Table has no primary key and REPLICA IDENTITY is not FULL (required for UPDATE/DELETE replication).")
            connector = "lakeflow_connect_postgres_logical"
            if blockers:
                status = "blocked"
            elif db_audit.db_cdc_enabled and (has_pk or ri == "full"):
                status = "ready"
            else:
                status = "needs_config"

        elif adapter.name == "oracle":
            if db_audit.db_cdc_enabled is False:
                blockers.append("Oracle ARCHIVELOG or minimal supplemental logging is not enabled.")
            if not has_pk:
                warnings.append("No primary key — configure ALL-column supplemental logging for this table.")
            connector = "lakeflow_connect_oracle_cdc"
            status = "blocked" if blockers else ("ready" if db_audit.db_cdc_enabled else "needs_config")

        elif adapter.name == "mysql":
            if db_audit.db_cdc_enabled is False:
                blockers.append("MySQL binlog_format is not ROW.")
            if not has_pk:
                warnings.append("No primary key — ensure binlog_row_image=FULL for reliable CDC deletes/updates.")
            connector = "lakeflow_connect_mysql_binlog"
            status = "blocked" if blockers else ("ready" if db_audit.db_cdc_enabled else "needs_config")

        elif adapter.name == "databricks":
            connector = "delta_change_data_feed"
            status = "ready"
            notes.append("Already on Databricks Delta; enable delta.enableChangeDataFeed=true for downstream incremental reads.")

        elif adapter.name == "snowflake":
            connector = "snowflake_stream_or_dynamic_table"
            status = "ready"
            notes.append("Already on Snowflake; enable CHANGE_TRACKING = TRUE for Streams or Dynamic Tables.")

        else:
            # File / warehouse / generic sources (DuckDB, SQLite, Snowflake, DB2, HANA, etc.)
            has_wm = any(
                c.canonical_type in {"timestamp", "date"}
                for c in tbl.columns
            )
            if not has_pk:
                warnings.append("No declared primary key — MERGE/UPSERT deduplication in Databricks Silver requires a surrogate or composite key.")
            if has_pk and has_wm:
                connector = "lakeflow_watermark_merge"
                status = "ready"
                notes.append("Primary key + temporal column present; ready for Lakeflow Auto CDC / watermark MERGE.")
            elif has_wm:
                connector = "lakeflow_watermark_append"
                status = "needs_config"
                notes.append("Temporal column present without PK; suitable for append-only ingestion or deduplicated Silver.")
            else:
                connector = "lakeflow_full_snapshot"
                status = "needs_config"
                notes.append("No log-based CDC or temporal watermark; ingest as full snapshot with Delta CDF diffing.")

        tbl.cdc_readiness = CdcReadiness(
            status=status,  # type: ignore[arg-type]
            connector_recommendation=connector,
            source_cdc_enabled=probe.cdc_enabled if probe else db_audit.db_cdc_enabled,
            source_change_tracking_enabled=probe.change_tracking_enabled if probe else db_audit.db_change_tracking_enabled,
            has_primary_key=has_pk,
            blockers=blockers,
            warnings=warnings,
            notes=notes,
        )

    return db_audit
