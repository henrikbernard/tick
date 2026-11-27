"""Regression tests covering all 10 bugs identified in Round-3 codebase review."""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from sqlalchemy import create_engine, text

from tick import DiscoveryConfig, diff_snapshots, discover_database, harmonize_snapshots
from tick.catalog import _introspect_object
from tick.dialects import DialectAdapter, sanitize_url
from tick.exporters import (
    render_harmonization_html,
    render_html_report,
    render_lakeflow_pipeline_py,
)
from tick.manufacturing import analyze_manufacturing_patterns
from tick.models import (
    ColumnInfo,
    ColumnProfile,
    DatabaseOverview,
    DiscoveryResult,
    ForeignKeyInfo,
    IndexInfo,
    TableInfo,
)
from tick.pii import detect_governance
from tick.profiler import ProfileArtifacts
from tick.recommendations import analyze_tables_and_quality


def _snap(tables: list[TableInfo]) -> DiscoveryResult:
    return DiscoveryResult(
        config=DiscoveryConfig(),
        overview=DatabaseOverview(
            dialect="duckdb",
            driver="duckdb_engine",
            database_name="test_db",
            sanitized_url="duckdb:///test.duckdb",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=len(tables),
            view_count=0,
            column_count=sum(len(t.columns) for t in tables),
        ),
        tables=tables,
    )


def test_round3_bug1_sap_stprs_and_semantic_governance_detection(tmp_path):
    """Bug #1: SAP STPRS/VERPR/NETPR and glossary-mapped semantic_name columns must trigger commercial_sensitive masks."""
    stprs = detect_governance("STPRS", "decimal")
    assert any(g.category == "commercial_sensitive" and g.uc_policy == "column_mask" for g in stprs)

    custom = detect_governance("Z_COL_99", "decimal", semantic_name="standard_price")
    assert any(g.category == "commercial_sensitive" and g.uc_policy == "column_mask" for g in custom)

    # End-to-end: MARA.STPRS in SQLite gets flagged automatically during discovery
    db_file = tmp_path / "sap_mara.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE MARA (MATNR TEXT PRIMARY KEY, STPRS DECIMAL(12, 2))"))
        conn.execute(text("INSERT INTO MARA VALUES ('MAT-1', 42.50), ('MAT-2', 99.00)"))
    engine.dispose()

    res = discover_database(str(db_file))
    mara = res.tables[0]
    stprs_col = next(c for c in mara.columns if c.name == "STPRS")
    assert stprs_col.profile is not None
    assert any(g.category == "commercial_sensitive" for g in stprs_col.profile.governance_findings)


def test_round3_bug2_diff_detects_breaking_type_narrowing():
    """Bug #2: Type narrowings within same canonical type (BIGINT->SMALLINT, VARCHAR(255)->VARCHAR(50), DECIMAL) are breaking."""
    before = _snap(
        [
            TableInfo(
                schema_name="main",
                name="metrics",
                full_name="main.metrics",
                columns=[
                    ColumnInfo(name="a_int", raw_type="BIGINT", canonical_type="integer"),
                    ColumnInfo(name="b_str", raw_type="VARCHAR(255)", canonical_type="string"),
                    ColumnInfo(name="c_dec", raw_type="DECIMAL(18, 4)", canonical_type="decimal"),
                    ColumnInfo(name="d_txt", raw_type="TEXT", canonical_type="string"),
                    ColumnInfo(name="e_safe_widen", raw_type="INT", canonical_type="integer"),
                ],
            )
        ]
    )
    after = _snap(
        [
            TableInfo(
                schema_name="main",
                name="metrics",
                full_name="main.metrics",
                columns=[
                    ColumnInfo(name="a_int", raw_type="SMALLINT", canonical_type="integer"),
                    ColumnInfo(name="b_str", raw_type="VARCHAR(50)", canonical_type="string"),
                    ColumnInfo(name="c_dec", raw_type="DECIMAL(10, 2)", canonical_type="decimal"),
                    ColumnInfo(name="d_txt", raw_type="VARCHAR(100)", canonical_type="string"),
                    ColumnInfo(name="e_safe_widen", raw_type="BIGINT", canonical_type="integer"),
                ],
            )
        ]
    )

    diff = diff_snapshots(before, after)
    by_col = {c.column: c for c in diff.column_changes}
    assert by_col["a_int"].breaking is True
    assert by_col["b_str"].breaking is True
    assert by_col["c_dec"].breaking is True
    assert by_col["d_txt"].breaking is True
    assert by_col["e_safe_widen"].breaking is False
    assert diff.breaking_changes_count == 4


def test_round3_bug3_introspect_object_and_view_def_rollback_on_inspector_errors():
    """Bug #3: Shared-connection inspector errors must call safe_rollback(conn) to prevent InFailedSqlTransaction."""
    conn = MagicMock()
    conn.in_transaction.return_value = True
    conn.dialect.identifier_preparer.quote.side_effect = lambda s: f'"{s}"'
    conn.execute.side_effect = RuntimeError("count error")
    conn.exec_driver_sql.side_effect = RuntimeError("count error")

    inspector = MagicMock()
    inspector.get_columns.side_effect = RuntimeError("cols error")
    inspector.get_pk_constraint.side_effect = RuntimeError("pk error")
    inspector.get_foreign_keys.side_effect = RuntimeError("fk error")
    inspector.get_indexes.side_effect = RuntimeError("idx error")
    inspector.get_unique_constraints.side_effect = RuntimeError("uq error")
    inspector.get_table_comment.side_effect = RuntimeError("comment error")
    inspector.get_view_definition.side_effect = RuntimeError("view error")

    adapter = DialectAdapter()
    _introspect_object(
        conn=conn,
        inspector=inspector,
        adapter=adapter,
        schema_name="public",
        default_schema="public",
        obj_name="orders",
        obj_type="table",
        stats=None,
        config=DiscoveryConfig(),
    )
    # 6 inspector failures + 1 exact count failure -> at least 7 rollbacks
    assert conn.rollback.call_count >= 7

    conn.reset_mock()
    conn.in_transaction.return_value = True
    assert adapter.get_view_definition(conn, inspector, "public", "v_orders") is None
    assert conn.rollback.call_count == 1


def test_round3_bug4_sanitize_url_masks_query_tokens_and_odbc_passwords():
    """Bug #4: sanitize_url must redact sensitive query parameters and ODBC PWD=... secrets."""
    dbx_url = "databricks://token:dapiRealSecret@adb-123.azuredatabricks.net?http_path=/sql/1.0/wh&access_token=dapiQuerySecret&catalog=mfg"
    clean_dbx = sanitize_url(dbx_url)
    assert "dapiRealSecret" not in clean_dbx
    assert "dapiQuerySecret" not in clean_dbx
    assert "catalog=mfg" in clean_dbx

    odbc_url = "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+18%7D%3BSERVER%3Ddb01%3BUID%3Dsa%3BPWD%3DSuperSecret99%3B"
    clean_odbc = sanitize_url(odbc_url)
    assert "SuperSecret99" not in clean_odbc

    pg_url = "postgresql://reader@db.internal:5432/prod?password=LeakMeNot&sslmode=require"
    clean_pg = sanitize_url(pg_url)
    assert "LeakMeNot" not in clean_pg
    assert "sslmode=require" in clean_pg


def test_round3_bug5_spaced_column_names_detected_for_watermarks_and_ot_telemetry(tmp_path):
    """Bug #5: SQL Server spaced column names ('Updated At', 'Tag ID', 'Quality Code') must match DE & OT detectors."""
    db_file = tmp_path / "spaced_ot.duckdb"
    engine = create_engine(f"duckdb:///{db_file}", future=True)
    base = datetime(2026, 6, 1, 9, 0, 0)
    with engine.begin() as conn:
        conn.execute(
            text(
                'CREATE TABLE "Sensor Readings" ('
                '"Tag ID" VARCHAR, '
                '"Event Time" TIMESTAMP, '
                '"Sensor Value" DOUBLE, '
                '"Quality Code" VARCHAR, '
                '"Updated At" TIMESTAMP)'
            )
        )
        for step in range(15):
            for tag_idx in range(3):
                ts = base + timedelta(seconds=step * 20 + tag_idx * 5)
                conn.execute(
                    text('INSERT INTO "Sensor Readings" VALUES (:t, :ts, :v, :q, :u)'),
                    {"t": f"T{tag_idx}", "ts": ts, "v": 10.0 + step, "q": "GOOD", "u": ts},
                )
    engine.dispose()

    res = discover_database(str(db_file))
    tbl = res.tables[0]
    assert tbl.recommendations is not None
    wm_cols = [w.column for w in tbl.recommendations.watermark_candidates]
    assert "Updated At" in wm_cols
    assert "Event Time" in wm_cols
    assert tbl.telemetry_profile is not None
    assert tbl.telemetry_profile.pattern == "narrow_historian_eav"
    assert tbl.telemetry_profile.tag_column == "Tag ID"
    assert tbl.telemetry_profile.quality_column == "Quality Code"
    assert tbl.telemetry_profile.median_interval_seconds == 20.0


def test_round3_bug6_cross_schema_same_table_name_fk_not_flagged_as_hierarchy():
    """Bug #6: FK from sales.items -> archive.items (different schema) is NOT a self-referential hierarchy."""
    tbl = TableInfo(
        schema_name="sales",
        name="items",
        full_name="sales.items",
        columns=[
            ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True),
            ColumnInfo(name="archive_item_id", raw_type="INT", canonical_type="integer", is_foreign_key=True),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["archive_item_id"],
                referred_schema="archive",
                referred_table="items",
                referred_columns=["id"],
            )
        ],
    )
    analyze_manufacturing_patterns([tbl], ProfileArtifacts())
    assert tbl.hierarchy is None


def test_round3_bug7_legacy_decimal_zeros_bare_date_time_and_julian_time_stitch():
    """Bug #7: Handle '.00' decimals, pair bare 'date'+'time', and stitch Julian date + HHMMSS into valid Spark code."""
    tbl = TableInfo(
        schema_name="main",
        name="jde_orders",
        full_name="main.jde_orders",
        columns=[
            ColumnInfo(name="date", raw_type="DECIMAL(8, 2)", canonical_type="decimal", profile=ColumnProfile()),
            ColumnInfo(name="time", raw_type="DECIMAL(6, 2)", canonical_type="decimal", profile=ColumnProfile()),
            ColumnInfo(name="ship_date", raw_type="INT", canonical_type="integer", profile=ColumnProfile()),
            ColumnInfo(name="ship_time", raw_type="INT", canonical_type="integer", profile=ColumnProfile()),
        ],
    )
    artifacts = ProfileArtifacts(
        raw_str_samples={
            ("main.jde_orders", "date"): ["20260315.00", "20260316.00", "20260317.00", "20260318.00"],
            ("main.jde_orders", "time"): ["083015.00", "094500.00", "141230.00", "170500.00"],
            ("main.jde_orders", "ship_date"): ["126074", "126075", "126076", "126077"],
            ("main.jde_orders", "ship_time"): ["083000", "093000", "103000", "113000"],
        }
    )
    analyze_manufacturing_patterns([tbl], artifacts)

    date_col = next(c for c in tbl.columns if c.name == "date")
    assert date_col.profile and date_col.profile.legacy_date
    assert date_col.profile.legacy_date.encoding == "yyyymmdd_int"
    assert date_col.profile.legacy_date.paired_column == "time"

    ship_col = next(c for c in tbl.columns if c.name == "ship_date")
    assert ship_col.profile and ship_col.profile.legacy_date
    assert ship_col.profile.legacy_date.encoding == "julian_cyyddd"
    assert ship_col.profile.legacy_date.paired_column == "ship_time"
    assert "F.to_timestamp" in ship_col.profile.legacy_date.spark_expression
    assert "ship_time" in ship_col.profile.legacy_date.spark_expression

    # Generated Lakeflow Python parses cleanly and references BOTH stitched expressions
    py_code = render_lakeflow_pipeline_py(_snap([tbl]))
    ast.parse(py_code)
    assert "'date_ts'" in py_code
    assert "'ship_date_ts'" in py_code


def test_round3_bug8_sizing_uses_full_table_span_and_accurate_subday_telemetry():
    """Bug #8: Large table sizing uses full-table min/max span; sub-day OT telemetry isn't clamped down to 1 day."""
    # Large 1,000,000-row table spanning 1,000 days -> ~1,000 rows/day (even if sample only spans 1 hour)
    big_tbl = TableInfo(
        schema_name="main",
        name="ledger",
        full_name="main.ledger",
        row_count=1_000_000,
        columns=[
            ColumnInfo(
                name="created_at",
                raw_type="TIMESTAMP",
                canonical_type="timestamp",
                profile=ColumnProfile(
                    min_value="2023-01-01T00:00:00+00:00",
                    max_value="2025-09-27T00:00:00+00:00",  # 1000 days
                ),
            )
        ],
    )
    # High-frequency OT telemetry table with 10,000 rows in 30 minutes -> 480,000 rows/day -> continuous_streaming
    base = datetime(2026, 6, 1, 8, 0, 0, tzinfo=timezone.utc)
    hist_tbl = TableInfo(
        schema_name="main",
        name="plc_tags",
        full_name="main.plc_tags",
        row_count=10_000,
        columns=[
            ColumnInfo(name="tag_id", raw_type="VARCHAR", canonical_type="string", profile=ColumnProfile()),
            ColumnInfo(
                name="recorded_at",
                raw_type="TIMESTAMP",
                canonical_type="timestamp",
                profile=ColumnProfile(
                    min_value=base.isoformat(),
                    max_value=(base + timedelta(minutes=30)).isoformat(),
                ),
            ),
            ColumnInfo(name="value", raw_type="DOUBLE", canonical_type="float", profile=ColumnProfile()),
        ],
    )
    artifacts = ProfileArtifacts(
        timestamp_samples={
            ("main.ledger", "created_at"): [base, base + timedelta(minutes=10)],
            ("main.plc_tags", "recorded_at"): [base, base + timedelta(minutes=15), base + timedelta(minutes=30)],
        }
    )
    analyze_manufacturing_patterns([big_tbl, hist_tbl], artifacts)

    assert big_tbl.ingestion_sizing is not None
    assert big_tbl.ingestion_sizing.estimated_daily_rows == 1000.0

    assert hist_tbl.ingestion_sizing is not None
    assert hist_tbl.ingestion_sizing.estimated_daily_rows == 480000.0
    assert hist_tbl.ingestion_sizing.recommended_cadence == "continuous_streaming"


def test_round3_bug9_composite_fk_and_pk_index_coverage():
    """Bug #9: Covered composite FKs must not trigger unindexed_fk; non-leading composite PK cols aren't marked indexed."""
    tbl = TableInfo(
        schema_name="main",
        name="line_items",
        full_name="main.line_items",
        row_count=500,
        primary_key=["tenant_id", "line_id"],
        columns=[
            ColumnInfo(name="tenant_id", raw_type="INT", canonical_type="integer", is_primary_key=True, is_indexed=True),
            ColumnInfo(name="line_id", raw_type="INT", canonical_type="integer", is_primary_key=True, is_indexed=False),
            ColumnInfo(name="order_id", raw_type="INT", canonical_type="integer", is_foreign_key=True, is_indexed=False),
        ],
        indexes=[IndexInfo(name="idx_tenant_order", columns=["tenant_id", "order_id"])],
        foreign_keys=[
            # Covered by composite index (tenant_id, order_id)
            ForeignKeyInfo(
                constrained_columns=["tenant_id", "order_id"],
                referred_table="orders",
                referred_columns=["tenant_id", "order_id"],
            ),
            # Unindexed single-col FK duplicated twice -> should only emit ONCE
            ForeignKeyInfo(
                constrained_columns=["order_id"],
                referred_table="orders_legacy",
                referred_columns=["id"],
            ),
            ForeignKeyInfo(
                constrained_columns=["order_id"],
                referred_table="orders_audit",
                referred_columns=["id"],
            ),
        ],
    )
    issues = analyze_tables_and_quality([tbl], [])
    unindexed = [i for i in issues if i.category == "unindexed_fk"]
    assert len(unindexed) == 1
    assert unindexed[0].column == "order_id"


def test_round3_bug10_html_svg_export_viewbox_size_fallback_and_heatmap_combo():
    """Bug #10: Exported SVGs include viewBox/width/height, KPI avoids est. 0.00 MB fallback, heatmap shows combined cast+missing."""
    t1 = TableInfo(
        schema_name="main",
        name="orders",
        full_name="main.orders",
        row_count=100,
        columns=[
            ColumnInfo(name="id", raw_type="INT", canonical_type="integer", spark_type="INT"),
            ColumnInfo(name="extra_a", raw_type="TEXT", canonical_type="string", spark_type="STRING"),
        ],
    )
    t2 = TableInfo(
        schema_name="main",
        name="orders",
        full_name="main.orders",
        row_count=100,
        columns=[
            ColumnInfo(name="id", raw_type="VARCHAR", canonical_type="string", spark_type="STRING"),
            ColumnInfo(name="extra_b", raw_type="TEXT", canonical_type="string", spark_type="STRING"),
        ],
    )
    html_disc = render_html_report(_snap([t1]))
    assert 'viewBox="${vx} ${vy} ${vw} ${vh}"' in html_disc
    assert "const hasSizing =" in html_disc
    assert 'el.type === "checkbox" ? "change" : "input"' in html_disc

    harm_html = render_harmonization_html(harmonize_snapshots({"p1": _snap([t1]), "p2": _snap([t2])}))
    assert "CAST(${casts}) -${missCols}C" in harm_html
    assert 'title="${esc(st.title)}"' in harm_html
