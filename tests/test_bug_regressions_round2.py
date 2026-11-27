"""Regression tests covering all 15 bugs identified in Round-2 codebase review."""

from __future__ import annotations

import ast
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import yaml
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from tick import DiscoveryConfig, diff_snapshots, discover_database, harmonize_snapshots
from tick.cli import app
from tick.databricks import lint_databricks_compatibility, map_to_spark_type
from tick.demo import create_manufacturing_demo_database
from tick.dialects import (
    DatabricksAdapter,
    DuckDBAdapter,
    MySQLAdapter,
    OracleAdapter,
    SQLiteAdapter,
    SqlServerAdapter,
    normalize_type,
)
from tick.exporters import (
    export_dbt_project,
    render_databricks_sizing_md,
    render_diff_markdown,
    render_html_report,
    render_lakeflow_pipeline_py,
    render_markdown_report,
    render_unity_catalog_ddl,
)
from tick.harmonize import render_harmonization_markdown
from tick.models import (
    ColumnChange,
    ColumnInfo,
    ColumnProfile,
    DatabaseOverview,
    DeltaCompatibility,
    DeltaCompatibilityIssue,
    DiscoveryResult,
    GovernanceFinding,
    LineageEdge,
    MetricDrift,
    PiiFinding,
    QualityIssue,
    RelationshipInfo,
    SnapshotDiff,
    TableInfo,
    TableRecommendations,
    ValueFrequency,
)
from tick.relationships import _extract_tables_from_sql, discover_relationships

runner = CliRunner()


def _minimal_result(
    tables: list[TableInfo],
    *,
    db_name: str = "test_db",
    dialect: str = "duckdb",
    relationships: list[RelationshipInfo] | None = None,
    lineage: list[LineageEdge] | None = None,
    quality_issues: list[QualityIssue] | None = None,
) -> DiscoveryResult:
    return DiscoveryResult(
        config=DiscoveryConfig(databricks_catalog="mfg", databricks_schema="bronze"),
        overview=DatabaseOverview(
            dialect=dialect,
            driver="duckdb_engine",
            database_name=db_name,
            sanitized_url="duckdb:///test.duckdb",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=sum(1 for t in tables if t.table_type == "table"),
            view_count=sum(1 for t in tables if t.table_type != "table"),
            column_count=sum(len(t.columns) for t in tables),
        ),
        tables=tables,
        relationships=relationships or [],
        lineage=lineage or [],
        quality_issues=quality_issues or [],
    )


def test_round2_bug1_uc_mask_and_row_filter_types_and_dedup():
    """Bug #1: UC masks/row filters must match column Spark types, deduplicate masks, and emit 1 row filter/table."""
    tbl = TableInfo(
        schema_name="main",
        name="cost_ledger",
        full_name="main.cost_ledger",
        columns=[
            ColumnInfo(
                name="plant_id",
                raw_type="INTEGER",
                canonical_type="integer",
                spark_type="INT",
                profile=ColumnProfile(
                    governance_findings=[
                        GovernanceFinding(
                            category="plant_isolation_key",
                            confidence="high",
                            uc_tag_key="data_domain",
                            uc_tag_value="plant_id",
                            uc_policy="row_filter",
                            reason="plant id",
                        )
                    ]
                ),
            ),
            ColumnInfo(
                name="WERKS",
                raw_type="VARCHAR",
                canonical_type="string",
                spark_type="STRING",
                profile=ColumnProfile(
                    governance_findings=[
                        GovernanceFinding(
                            category="plant_isolation_key",
                            confidence="high",
                            uc_tag_key="data_domain",
                            uc_tag_value="plant_id",
                            uc_policy="row_filter",
                            reason="sap plant",
                        )
                    ]
                ),
            ),
            ColumnInfo(
                name="unit_cost",
                raw_type="DECIMAL(12, 2)",
                canonical_type="decimal",
                spark_type="DECIMAL(12, 2)",
                profile=ColumnProfile(
                    governance_findings=[
                        GovernanceFinding(
                            category="commercial_sensitive",
                            confidence="high",
                            uc_tag_key="sensitivity",
                            uc_tag_value="commercial_cost_margin",
                            uc_policy="column_mask",
                            reason="cost",
                        )
                    ]
                ),
            ),
            ColumnInfo(
                name="contact_email",
                raw_type="VARCHAR",
                canonical_type="string",
                spark_type="STRING",
                profile=ColumnProfile(
                    pii_findings=[PiiFinding(category="email", confidence="high", score=0.95, reason="email")],
                    governance_findings=[
                        GovernanceFinding(
                            category="ip_or_export_controlled",
                            confidence="high",
                            uc_tag_key="compliance",
                            uc_tag_value="itar_ear_ip",
                            uc_policy="column_mask",
                            reason="itar",
                        )
                    ],
                ),
            ),
        ],
    )
    ddl = render_unity_catalog_ddl(_minimal_result([tbl]))

    # Non-string DECIMAL column gets a matching DECIMAL(12, 2) mask function, NOT mask_sensitive_string
    assert "CREATE OR REPLACE FUNCTION `mfg`.`bronze`.mask_sensitive_decimal_12_2(val DECIMAL(12, 2))" in ddl
    assert "RETURNS DECIMAL(12, 2)" in ddl
    assert "ALTER COLUMN `unit_cost` SET MASK `mfg`.`bronze`.mask_sensitive_decimal_12_2;" in ddl
    assert "ALTER COLUMN `unit_cost` SET MASK `mfg`.`bronze`.mask_sensitive_string" not in ddl

    # Column with both PII and governance column_mask is emitted ONLY once
    assert ddl.count("ALTER COLUMN `contact_email` SET MASK") == 1
    assert "pii_email, itar_ear_ip" in ddl

    # Non-string INT plant key gets INT row filter function, and table gets ONLY 1 SET ROW FILTER
    assert "CREATE OR REPLACE FUNCTION `mfg`.`bronze`.plant_row_filter_int(plant_val INT)" in ddl
    assert ddl.count("SET ROW FILTER") == 1
    assert "Additional plant keys skipped" in ddl


def test_round2_bug2_type_normalization_and_spark_decimal_overflow():
    """Bug #2: Prevent vendor SQL type misclassifications and 32-bit Spark INT overflow on large DECIMAL(p,0)."""
    assert normalize_type("SQL_VARIANT") == "other"
    assert normalize_type("BITMAP") == "other"
    assert normalize_type("INTERVAL") == "other"
    assert normalize_type("INTERVAL DAY TO SECOND") == "other"
    assert normalize_type("POINT") == "other"
    assert normalize_type("LONG") == "string"
    assert normalize_type("LONG RAW") == "binary"
    assert normalize_type("LONGBLOB") == "binary"
    assert normalize_type("LONGTEXT") == "string"
    assert normalize_type("DECIMAL(18, 0)") == "integer"
    assert normalize_type("NUMBER(38, 0)") == "decimal"
    assert normalize_type("DECIMAL(40, 0)") == "decimal"

    assert map_to_spark_type("DECIMAL(18, 0)", "integer") == "BIGINT"
    assert map_to_spark_type("DECIMAL(9, 0)", "integer") == "INT"
    assert map_to_spark_type("NUMBER(38, 0)", "decimal") == "DECIMAL(38, 0)"
    assert map_to_spark_type("SQL_VARIANT", "other") == "STRING"

    tbl = TableInfo(
        schema_name="main",
        name="erp_ids",
        full_name="main.erp_ids",
        columns=[
            ColumnInfo(name="huge_id", raw_type="DECIMAL(40, 0)", canonical_type=normalize_type("DECIMAL(40, 0)")),
            ColumnInfo(name="big_id", raw_type="DECIMAL(18, 0)", canonical_type=normalize_type("DECIMAL(18, 0)")),
            ColumnInfo(name="exotic", raw_type="SQL_VARIANT", canonical_type=normalize_type("SQL_VARIANT")),
        ],
    )
    lint_databricks_compatibility([tbl])
    assert tbl.delta_compatibility is not None
    cats = {(i.column, i.category) for i in tbl.delta_compatibility.issues}
    assert ("huge_id", "decimal_precision_overflow") in cats
    assert ("exotic", "unsupported_type") in cats
    assert tbl.columns[1].spark_type == "BIGINT"
    assert tbl.columns[2].spark_type == "STRING"


def test_round2_bug3_exact_aggs_sampled_nulls_not_flagged_all_null(tmp_path):
    """Bug #3: Column NULL in sampled rows but populated later in table must NOT be flagged all_null."""
    db_file = tmp_path / "late_col.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, late_col TEXT)"))
        for i in range(1, 21):
            val = None if i <= 10 else f"val_{i}"
            conn.execute(text("INSERT INTO items VALUES (:i, :v)"), {"i": i, "v": val})
    engine.dispose()

    res = discover_database(str(db_file), config=DiscoveryConfig(profile_mode="full", sample_size=10))
    tbl = res.tables[0]
    late_col = next(c for c in tbl.columns if c.name == "late_col")
    assert late_col.profile is not None
    assert late_col.profile.sample_size == 20
    assert late_col.profile.non_null_count == 10
    assert late_col.profile.null_rate == 0.5
    assert late_col.profile.cardinality_class == "unknown"
    assert not any(
        i.category == "all_null_column" and i.column == "late_col" for i in res.quality_issues
    )


def test_round2_bug4_lakeflow_spark_sql_escaping_and_full_refresh_batch():
    """Bug #4: Escape backslashes/newlines/docstrings in Lakeflow DLT and use batch reads for full_refresh."""
    full_tbl = TableInfo(
        schema_name="main",
        name="ref_codes",
        full_name="main.ref_codes",
        columns=[
            ColumnInfo(
                name="status",
                raw_type="VARCHAR",
                canonical_type="string",
                profile=ColumnProfile(
                    sample_size=20,
                    non_null_count=20,
                    null_rate=0.0,
                    distinct_count=3,
                    cardinality_class="low",
                    top_values=[
                        ValueFrequency(value=r"C:\Plant\Line1\\", count=10, rate=0.5),
                        ValueFrequency(value="O'Brien\\Shift", count=5, rate=0.25),
                        ValueFrequency(value="LINE1\nLINE2", count=5, rate=0.25),
                    ],
                ),
            )
        ],
        recommendations=TableRecommendations(
            entity_role="reference",
            ingestion_strategy="full_refresh",
            ingestion_strategy_reason="Small reference table",
        ),
    )
    inc_tbl = TableInfo(
        schema_name="main",
        name="events",
        full_name="main.events",
        primary_key=["id"],
        columns=[
            ColumnInfo(name="id", raw_type="BIGINT", canonical_type="integer", is_primary_key=True),
            ColumnInfo(name="updated_at", raw_type="TIMESTAMP", canonical_type="timestamp"),
        ],
        recommendations=TableRecommendations(
            entity_role="fact",
            ingestion_strategy="incremental_merge",
            ingestion_strategy_reason="PK + updated_at",
        ),
    )
    res = _minimal_result([full_tbl, inc_tbl], db_name='erp """prod"""')
    py_code = render_lakeflow_pipeline_py(res, catalog='cat"""1', bronze_schema="bronze", silver_schema="silver")

    # Generated Python parses cleanly even with triple quotes and backslashes
    tree = ast.parse(py_code)
    assert tree is not None

    # Full-refresh table uses batch reads (Materialized View), NOT readStream/read_stream
    ref_section = py_code.split("# --- main.events")[0]
    assert "spark.read.table('cat\"\"\"1.bronze.ref_codes')" in ref_section
    assert "return dlt.read('clean_ref_codes')" in ref_section
    assert "readStream" not in ref_section
    assert "read_stream" not in ref_section

    # Spark SQL literal escapes backslashes and newlines so trailing backslash cannot break closing quote
    assert r"\\\\'" in ref_section or r"\\" in ref_section
    assert r"\n" in ref_section

    # Incremental table continues to use streaming reads
    inc_section = py_code.split("# --- main.events")[1]
    assert "spark.readStream.table" in inc_section


def test_round2_bug5_relationship_sample_cap_fast_mode_and_decimal_target():
    """Bug #5: Don't falsely reject FKs when target sample is incomplete; allow unique decimal target keys."""
    tgt = TableInfo(
        schema_name="main",
        name="vendors",
        full_name="main.vendors",
        row_count=3500,
        columns=[
            ColumnInfo(
                name="vendor_id",
                raw_type="NUMBER(20, 0)",
                canonical_type="decimal",
                profile=ColumnProfile(
                    sample_size=2000,
                    non_null_count=2000,
                    null_rate=0.0,
                    distinct_count=2000,
                    cardinality_class="unique",
                ),
            )
        ],
    )
    src = TableInfo(
        schema_name="main",
        name="purchase_orders",
        full_name="main.purchase_orders",
        row_count=500,
        columns=[
            ColumnInfo(name="po_id", raw_type="INT", canonical_type="integer", is_primary_key=True),
            ColumnInfo(name="vendor_id", raw_type="NUMBER(20, 0)", canonical_type="decimal"),
        ],
        primary_key=["po_id"],
    )

    # Disjoint sample values because target had 3500 rows (> 2000 sample cap)
    samples = {
        ("main.vendors", "vendor_id"): {"1", "2", "3"},
        ("main.purchase_orders", "vendor_id"): {"3001", "3002", "3003"},
    }

    # Explicit value_sample_complete=False must NOT reject the relationship
    rels, _ = discover_relationships(
        [tgt, src],
        samples,
        DiscoveryConfig(profile_mode="full", sample_size=5000),
        value_sample_complete={("main.vendors", "vendor_id"): False},
    )
    assert any(
        r.source_table == "main.purchase_orders" and r.target_table == "main.vendors"
        for r in rels
    )

    # Fallback without value_sample_complete in fast mode (row_count=1500 > 1000 fast cap) must NOT reject
    tgt.row_count = 1500
    rels_fast, _ = discover_relationships(
        [tgt, src],
        samples,
        DiscoveryConfig(profile_mode="fast", sample_size=5000),
    )
    assert any(
        r.source_table == "main.purchase_orders" and r.target_table == "main.vendors"
        for r in rels_fast
    )


def test_round2_bug6_diff_snapshots_ignores_unprofiled_and_errored_null_rates():
    """Bug #6: Unprofiled (sample_size=0) or errored columns must not trigger false null-rate regressions."""
    before = _minimal_result(
        [
            TableInfo(
                schema_name="main",
                name="users",
                full_name="main.users",
                columns=[
                    ColumnInfo(
                        name="email",
                        raw_type="TEXT",
                        canonical_type="string",
                        profile=ColumnProfile(
                            sample_size=0,
                            null_rate=0.0,
                            pii_findings=[
                                PiiFinding(category="email", confidence="medium", score=0.75, reason="name")
                            ],
                        ),
                    ),
                    ColumnInfo(
                        name="broken_col",
                        raw_type="TEXT",
                        canonical_type="string",
                        profile=ColumnProfile(profile_error="timeout"),
                    ),
                    ColumnInfo(
                        name="real_drift_col",
                        raw_type="TEXT",
                        canonical_type="string",
                        profile=ColumnProfile(sample_size=100, non_null_count=95, null_count=5, null_rate=0.05),
                    ),
                ],
            )
        ]
    )
    after = _minimal_result(
        [
            TableInfo(
                schema_name="main",
                name="users",
                full_name="main.users",
                columns=[
                    ColumnInfo(
                        name="email",
                        raw_type="TEXT",
                        canonical_type="string",
                        profile=ColumnProfile(sample_size=100, non_null_count=50, null_count=50, null_rate=0.50),
                    ),
                    ColumnInfo(
                        name="broken_col",
                        raw_type="TEXT",
                        canonical_type="string",
                        profile=ColumnProfile(sample_size=100, non_null_count=40, null_count=60, null_rate=0.60),
                    ),
                    ColumnInfo(
                        name="real_drift_col",
                        raw_type="TEXT",
                        canonical_type="string",
                        profile=ColumnProfile(sample_size=100, non_null_count=50, null_count=50, null_rate=0.50),
                    ),
                ],
            )
        ]
    )
    diff = diff_snapshots(before, after)
    drifted_cols = [m.column for m in diff.metric_drifts if m.metric == "null_rate"]
    assert drifted_cols == ["real_drift_col"]


def test_round2_bug7_safe_session_guardrails_and_adapter_rollbacks(tmp_path):
    """Bug #7: Safe session guardrails apply across catalog, DDIC glossary, profiling, and CDC; adapters rollback on errors."""
    db_file = tmp_path / "sap_guard.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE DD02T (tabname TEXT, ddlanguage TEXT, ddtext TEXT)"))
        conn.execute(text("INSERT INTO DD02T VALUES ('ZTEST', 'E', 'Test SAP table')"))
        conn.execute(text("CREATE TABLE ZTEST (id INTEGER PRIMARY KEY, val TEXT)"))
        conn.execute(text("INSERT INTO ZTEST VALUES (1, 'ok')"))
    engine.dispose()

    calls: list[tuple[bool, int | None]] = []
    orig_configure = SQLiteAdapter.configure_safe_session

    def spy_configure(self, conn, read_uncommitted=False, query_timeout_seconds=None):
        calls.append((read_uncommitted, query_timeout_seconds))
        return orig_configure(self, conn, read_uncommitted=read_uncommitted, query_timeout_seconds=query_timeout_seconds)

    with patch.object(SQLiteAdapter, "configure_safe_session", spy_configure):
        discover_database(
            str(db_file),
            config=DiscoveryConfig(
                read_uncommitted=True,
                query_timeout_seconds=12,
                audit_cdc=True,
                detect_manufacturing=True,
            ),
        )

    # Called for: version check, catalog, SAP DDIC glossary, profiling, and CDC audit (>= 5)
    assert len(calls) >= 5
    assert all(c == (True, 12) for c in calls)

    # Non-Postgres adapters must rollback active transactions on caught SQL errors
    for adapter in (
        SqlServerAdapter(),
        DuckDBAdapter(),
        SQLiteAdapter(),
        MySQLAdapter(),
        DatabricksAdapter(),
        OracleAdapter(),
    ):
        conn = MagicMock()
        conn.in_transaction.return_value = True
        conn.execute.side_effect = RuntimeError("db error")
        if isinstance(adapter, OracleAdapter):
            adapter.audit_database_cdc(conn)
        else:
            adapter.get_table_stats(conn, "dbo")
        assert conn.rollback.call_count >= 1, f"{type(adapter).__name__} did not rollback on SQL error"


def test_round2_bug8_max_tables_breaks_outer_schema_loop(tmp_path):
    """Bug #8: --max-tables must stop introspecting remaining schemas and avoid empty SchemaSummary entries."""
    db_file = tmp_path / "multi_schema.duckdb"
    engine = create_engine(f"duckdb:///{db_file}", future=True)
    with engine.begin() as conn:
        for s in ("s1", "s2", "s3"):
            conn.execute(text(f"CREATE SCHEMA {s}"))
            conn.execute(text(f"CREATE TABLE {s}.t1 (id INT)"))
            conn.execute(text(f"CREATE TABLE {s}.t2 (id INT)"))
    engine.dispose()

    queried_schemas: list[str] = []
    orig_stats = DuckDBAdapter.get_table_stats

    def spy_stats(self, conn, schema_name):
        queried_schemas.append(schema_name)
        return orig_stats(self, conn, schema_name)

    with patch.object(DuckDBAdapter, "get_table_stats", spy_stats):
        res = discover_database(
            str(db_file),
            config=DiscoveryConfig(
                include_schemas=["s1", "s2", "s3"],
                max_tables=2,
                profile_mode="none",
            ),
        )

    assert len(res.tables) == 2
    assert len(res.schemas) == 1
    assert queried_schemas == ["s1"]


def test_round2_bug9_harmonize_decimal_scale_pk_drift_and_safe_site_dedup():
    """Bug #9: Preserve DECIMAL scale > 6, render PK-only drift in Markdown, escape bronze_schema, deduplicate safe_site."""
    p1 = _minimal_result(
        [
            TableInfo(
                schema_name="main",
                name="specs",
                full_name="main.specs",
                primary_key=["id"],
                columns=[
                    ColumnInfo(name="id", raw_type="INT", canonical_type="integer", spark_type="INT"),
                    ColumnInfo(name="tolerance", raw_type="DECIMAL(12, 8)", canonical_type="decimal", spark_type="DECIMAL(12, 8)"),
                ],
            ),
            TableInfo(
                schema_name="main",
                name="pk_only",
                full_name="main.pk_only",
                primary_key=["id"],
                columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", spark_type="INT")],
            ),
        ],
        db_name="plant_1",
    )
    p2 = _minimal_result(
        [
            TableInfo(
                schema_name="main",
                name="specs",
                full_name="main.specs",
                primary_key=["id"],
                columns=[
                    ColumnInfo(name="id", raw_type="INT", canonical_type="integer", spark_type="INT"),
                    ColumnInfo(name="tolerance", raw_type="DECIMAL(10, 2)", canonical_type="decimal", spark_type="DECIMAL(10, 2)"),
                ],
            ),
            TableInfo(
                schema_name="main",
                name="pk_only",
                full_name="main.pk_only",
                primary_key=["id", "site"],
                columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", spark_type="INT")],
            ),
        ],
        db_name="plant_2",
    )

    report = harmonize_snapshots({"plant-1": p1, "plant_1": p2}, catalog="mfg", bronze_schema="bronze`raw")
    htbls = {t.table_name: t for t in report.tables}

    # DECIMAL(12, 8) + DECIMAL(10, 2) -> DECIMAL(16, 8), never truncated to DECIMAL(38, 6)
    tol_col = next(c for c in htbls["specs"].columns if c.sanitized_name == "tolerance")
    assert tol_col.target_spark_type == "DECIMAL(16, 8)"

    # Distinct safe_site schemas & escaped backticks in bronze_schema
    sql = htbls["specs"].unified_sql
    assert "`bronze``raw_plant_1`" in sql
    assert "`bronze``raw_plant_1_2`" in sql

    # PK-only drift table is rendered in Markdown report
    md = render_harmonization_markdown(report)
    assert "### `pk_only` — Harmonization Plan" in md
    assert "Primary Key Divergence" in md


def test_round2_bug10_multitag_historian_per_tag_cadence(tmp_path):
    """Bug #10: Staggered multi-tag narrow historian must compute per-tag cadence, not cross-tag inter-arrival."""
    db_file = tmp_path / "staggered_hist.duckdb"
    engine = create_engine(f"duckdb:///{db_file}", future=True)
    base_ts = datetime(2026, 6, 1, 8, 0, 0, tzinfo=timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE historian_tags (tag_id VARCHAR, recorded_at TIMESTAMP, numeric_val DOUBLE, opc_quality VARCHAR)"
            )
        )
        # 5 sensors each polling every 60s, staggered by 12s
        for step in range(20):
            for tag_idx in range(5):
                ts = base_ts + timedelta(seconds=step * 60 + tag_idx * 12)
                conn.execute(
                    text("INSERT INTO historian_tags VALUES (:t, :ts, :v, 'GOOD')"),
                    {"t": f"TAG_{tag_idx}", "ts": ts.replace(tzinfo=None), "v": 40.0 + step * 0.5 + tag_idx},
                )
    engine.dispose()

    res = discover_database(str(db_file), config=DiscoveryConfig(profile_mode="full", sample_size=500))
    hist = res.tables[0]
    assert hist.telemetry_profile is not None
    assert hist.telemetry_profile.pattern == "narrow_historian_eav"
    assert hist.telemetry_profile.median_interval_seconds == 60.0


def test_round2_bug11_html_lineage_cycle_guard_and_escaped_clustering():
    """Bug #11: HTML lineage breaks cycles/self-loops and escapes Liquid Clustering keys."""
    tbl = TableInfo(
        schema_name="main",
        name="bom",
        full_name="main.bom",
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer")],
        delta_compatibility=DeltaCompatibility(
            liquid_clustering_keys=["<img src=x onerror=alert(1)>"]
        ),
    )
    res = _minimal_result(
        [tbl],
        lineage=[
            LineageEdge(upstream_table="main.bom", downstream_table="main.bom", edge_type="foreign_key"),
            LineageEdge(upstream_table="main.a", downstream_table="main.b", edge_type="foreign_key"),
            LineageEdge(upstream_table="main.b", downstream_table="main.a", edge_type="foreign_key"),
        ],
    )
    html = render_html_report(res)
    assert 'esc((dc.liquid_clustering_keys || []).join(", "))' in html
    assert '${(dc.liquid_clustering_keys || []).join(", ")' not in html
    assert "const backEdges = new Set();" in html
    assert "e.upstream_table === e.downstream_table" in html


def test_round2_bug12_dbt_dangling_fk_quotes_and_staging_collision(tmp_path):
    """Bug #12: Omit dbt relationships tests for excluded targets, escape single quotes, deduplicate staging files."""
    t1 = TableInfo(
        schema_name="main",
        name="work-orders",
        full_name="main.work-orders",
        primary_key=["id"],
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
    )
    t2 = TableInfo(
        schema_name="main",
        name="work_orders",
        full_name="main.work_orders",
        primary_key=["id"],
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
    )
    t3 = TableInfo(
        schema_name="main",
        name="plant's_items",
        full_name="main.plant's_items",
        columns=[
            ColumnInfo(name="wo_id", raw_type="INT", canonical_type="integer"),
            ColumnInfo(name="missing_id", raw_type="INT", canonical_type="integer"),
        ],
    )
    res = _minimal_result(
        [t1, t2, t3],
        relationships=[
            RelationshipInfo(
                source_table="main.plant's_items",
                source_column="wo_id",
                target_table="main.work_orders",
                target_column="id",
                kind="explicit_fk",
                confidence=1.0,
            ),
            RelationshipInfo(
                source_table="main.plant's_items",
                source_column="missing_id",
                target_table="main.excluded_parent",
                target_column="id",
                kind="explicit_fk",
                confidence=1.0,
            ),
        ],
    )
    out_dir = tmp_path / "dbt_out"
    export_dbt_project(res, out_dir)

    parsed_yaml = yaml.safe_load((out_dir / "sources.yml").read_text(encoding="utf-8"))
    items_entry = next(
        t for t in parsed_yaml["sources"][0]["tables"] if t["name"] == "plant's_items"
    )
    cols = {c["name"]: c for c in items_entry["columns"]}
    assert "data_tests" in cols["wo_id"]
    assert "data_tests" not in cols["missing_id"]

    # Both colliding staging models exist without overwriting each other
    staging_files = sorted(p.name for p in (out_dir / "staging").glob("*.sql"))
    assert "stg_main__work_orders.sql" in staging_files
    assert "stg_main__work_orders_2.sql" in staging_files

    # Single quote escaped in Jinja source() call
    items_sql = (out_dir / "staging" / "stg_main__plant_s_items.sql").read_text(encoding="utf-8")
    assert "source('test_db', 'plant\\'s_items')" in items_sql


def test_round2_bug13_markdown_and_mermaid_escaping():
    """Bug #13: Guard digit-leading Mermaid IDs, double quotes in Mermaid labels, and pipes in Markdown tables."""
    tbl = TableInfo(
        schema_name="123_sch",
        name='pipe|tbl"1',
        full_name='123_sch.pipe|tbl"1',
        columns=[
            ColumnInfo(
                name='col|"a',
                raw_type="VARCHAR|10",
                canonical_type="string",
                spark_type="STRING",
                semantic_name="Pipe|Name",
            )
        ],
        delta_compatibility=DeltaCompatibility(
            issues=[
                DeltaCompatibilityIssue(
                    severity="high",
                    category="illegal_column_name",
                    column='col|"a',
                    message='Bad | char in "col"',
                    remediation="Rename | clean",
                )
            ]
        ),
    )
    view = TableInfo(
        schema_name="123_sch",
        name='99_v|"1',
        full_name='123_sch.99_v|"1',
        table_type="view",
        columns=[ColumnInfo(name="x", raw_type="INT", canonical_type="integer")],
    )
    res = _minimal_result(
        [tbl, view],
        relationships=[
            RelationshipInfo(
                source_table='123_sch.pipe|tbl"1',
                source_column='col|"a',
                target_table='123_sch.pipe|tbl"1',
                target_column='col|"a',
                kind="explicit_fk",
                confidence=1.0,
            )
        ],
        lineage=[
            LineageEdge(
                upstream_table='123_sch.pipe|tbl"1',
                downstream_table='123_sch.99_v|"1',
                edge_type="view_dependency",
            )
        ],
        quality_issues=[
            QualityIssue(
                severity="high",
                category="test",
                table='123_sch.pipe|tbl"1',
                column='col|"a',
                message="Issue | detail",
            )
        ],
    )

    md = render_markdown_report(res)
    # Digit-leading Mermaid IDs are prefixed with n_
    assert "    n_123_sch_pipe_tbl_1 {" in md
    assert '    n_123_sch_pipe_tbl_1[("123_sch.pipe|tbl\'1")]' in md
    # Pipes escaped in Markdown table rows
    assert r"`col\|\"a`" not in md  # quotes don't need backslash, pipes do
    assert r"pipe\|tbl" in md
    assert r"VARCHAR\|10" in md
    assert r"Pipe\|Name" in md

    # Databricks sizing Markdown escapes pipes in lint messages & table/column names
    sizing_md = render_databricks_sizing_md(res)
    assert r"Bad \| char" in sizing_md
    assert r"Rename \| clean" in sizing_md

    # Diff Markdown escapes pipes in table/column/before/after cells
    diff = SnapshotDiff(
        before_discovered_at="t1",
        after_discovered_at="t2",
        before_database="db",
        after_database="db",
        column_changes=[
            ColumnChange(
                table="s.a|b",
                column="c|d",
                change_type="type_changed",
                before="T|1",
                after="T|2",
                breaking=True,
            )
        ],
        metric_drifts=[
            MetricDrift(table="s.a|b", column="c|d", metric="null_rate", before=0.0, after=50.0, delta=50.0)
        ],
    )
    diff_md = render_diff_markdown(diff)
    assert r"`s.a\|b`" in diff_md
    assert r"`T\|1`" in diff_md


def test_round2_bug14_compare_sources_snapshot_json_labels(tmp_path):
    """Bug #14: compare-sources uses parent directory names instead of 'snapshot' / 'snapshot_2' for */snapshot.json."""
    ohio_db = create_manufacturing_demo_database(tmp_path / "o.duckdb", plant_variant="plant_ohio")
    munich_db = create_manufacturing_demo_database(tmp_path / "m.duckdb", plant_variant="plant_munich")

    ohio_dir = tmp_path / "ohio"
    munich_dir = tmp_path / "munich"
    ohio_dir.mkdir()
    munich_dir.mkdir()

    r1 = runner.invoke(app, ["discover", str(ohio_db), "--json", str(ohio_dir / "snapshot.json"), "--quiet"])
    r2 = runner.invoke(app, ["discover", str(munich_db), "--json", str(munich_dir / "snapshot.json"), "--quiet"])
    assert r1.exit_code == 0 and r2.exit_code == 0

    out_json = tmp_path / "harm.json"
    r3 = runner.invoke(
        app,
        ["compare-sources", str(ohio_dir / "snapshot.json"), str(munich_dir / "snapshot.json"), "--json", str(out_json)],
    )
    assert r3.exit_code == 0
    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["sources"] == ["ohio", "munich"]


def test_round2_bug15_view_lineage_excludes_create_view_target():
    """Bug #15: _extract_tables_from_sql must exclude the target view in CREATE VIEW <target> AS SELECT ..."""
    sql1 = "CREATE VIEW analytics.v_sales AS SELECT o.id FROM raw.orders o JOIN raw.customers c ON o.cid = c.id"
    refs1 = _extract_tables_from_sql(sql1, "postgres")
    assert ("analytics", "v_sales") not in refs1
    assert ("raw", "orders") in refs1
    assert ("raw", "customers") in refs1

    sql2 = "CREATE OR REPLACE VIEW main.v_rollup AS WITH cte AS (SELECT * FROM raw.events) SELECT * FROM cte"
    refs2 = _extract_tables_from_sql(sql2, "duckdb")
    assert ("main", "v_rollup") not in refs2
    assert (None, "cte") not in refs2
    assert ("raw", "events") in refs2
