"""Regression tests covering every bug identified during codebase review."""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from sqlalchemy import create_engine, text

from tick import DiscoveryConfig, discover_database, harmonize_snapshots
from tick.databricks import deduplicate_column_names, deduplicate_table_names
from tick.demo import create_demo_database
from tick.dialects import PostgresAdapter, SqlServerAdapter, safe_rollback
from tick.exporters import (
    export_dbt_project,
    render_html_report,
    render_lakeflow_pipeline_py,
    render_unity_catalog_ddl,
)
from tick.models import (
    ColumnInfo,
    ColumnProfile,
    DatabaseOverview,
    DiscoveryResult,
    GovernanceFinding,
    TableInfo,
    ValueFrequency,
)
from tick.pii import detect_pii


def test_regression_1_exclude_table_filters_both_short_and_full_names(tmp_path):
    """Bug #1: --exclude-table previously failed due to OR across short and full names."""
    db_path = create_demo_database(tmp_path / "filter_test.sqlite")

    # Exclude by short name glob
    res_short = discover_database(
        str(db_path),
        config=DiscoveryConfig(profile_mode="none", exclude_tables=["legacy_*", "empty_*"]),
    )
    names_short = {t.name for t in res_short.tables}
    assert "legacy_imports" not in names_short
    assert "empty_staging_queue" not in names_short
    assert "customers" in names_short

    # Exclude by schema-qualified full_name glob
    res_full = discover_database(
        str(db_path),
        config=DiscoveryConfig(profile_mode="none", exclude_tables=["main.orders", "main.v_*"]),
    )
    names_full = {t.name for t in res_full.tables}
    assert "orders" not in names_full
    assert "v_order_summary" not in names_full
    assert "v_customer_ltv" not in names_full
    assert "customers" in names_full


def test_regression_2_safe_rollback_clears_failed_transaction_on_postgres_adapter():
    """Bug #2: Caught SQL errors on Postgres must rollback so subsequent queries don't hit InFailedSqlTransaction."""
    conn = MagicMock()
    conn.in_transaction.return_value = True
    conn.execute.side_effect = RuntimeError("permission denied")

    pg = PostgresAdapter()
    audit = pg.audit_database_cdc(conn)
    assert audit.wal_level is None
    assert conn.rollback.call_count >= 1

    conn.reset_mock()
    conn.in_transaction.return_value = True
    conn.execute.side_effect = RuntimeError("pg_matviews error")
    assert pg.get_materialized_views(conn, "public") == []
    assert conn.rollback.call_count == 1

    # Also test safe_rollback helper directly when not in transaction
    conn2 = MagicMock()
    conn2.in_transaction.return_value = False
    safe_rollback(conn2)
    conn2.rollback.assert_not_called()


def test_regression_3_mssql_tablesample_precedes_nolock():
    """Bug #3: In T-SQL, TABLESAMPLE must come before WITH (NOLOCK)."""
    ms = SqlServerAdapter()
    sql = ms.build_sample_query(
        "[dbo].[orders]", "[id]", 100, 10_000, use_tablesample=True, read_uncommitted=True
    )
    assert sql.index("TABLESAMPLE") < sql.index("WITH (NOLOCK)")
    assert sql.endswith("TABLESAMPLE (200 ROWS) WITH (NOLOCK)")


def test_regression_4_lakeflow_paired_date_rename_and_special_char_escaping(tmp_path):
    """Bug #4: Paired legacy date+time columns must rename BOTH columns, and quotes/backslashes must not break Python AST."""
    db_file = tmp_path / "special_chars.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                'CREATE TABLE "Plant\'s Orders" ('
                '"Order #" INTEGER PRIMARY KEY, '
                '"Mfg Date" INTEGER NOT NULL, '
                '"Mfg Time" INTEGER NOT NULL, '
                '"Label ""Quote""" VARCHAR(50) NOT NULL)'
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO "Plant's Orders" VALUES
                (1, 20261120, 83015, 'A "1"'),
                (2, 20261121, 142500, 'B \\\\ 2'),
                (3, 20261122, 235959, 'A "1"'),
                (4, 20261123, 91500, 'B \\\\ 2')
                """
            )
        )
    engine.dispose()

    res = discover_database(str(db_file))
    py_code = render_lakeflow_pipeline_py(res)
    # Must be 100% syntactically valid Python even with quotes/backslashes in names & values!
    ast.parse(py_code)
    # Paired time column must reference renamed 'mfg_time', NOT raw 'Mfg Time'!
    assert "F.col('mfg_date')" in py_code
    assert "F.col('mfg_time')" in py_code
    assert "F.col('Mfg Time')" not in py_code


def test_regression_5_duplicate_sanitized_column_and_table_names(tmp_path):
    """Bug #5: Colliding sanitized column names and multi-schema table names must be deduplicated across exporters."""
    # Column deduplication
    col_map = deduplicate_column_names(["Order #", "Order!", "Qty/Hr", "Qty-Hr"])
    assert len(set(col_map.values())) == 4
    assert col_map["Order #"] == "order"
    assert col_map["Order!"] == "order_2"

    # End-to-end DB with colliding sanitized column names + digit-prefixed column
    db_file = tmp_path / "collisions.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE colliding_cols (
                    id INTEGER PRIMARY KEY,
                    "Order #" VARCHAR(20),
                    "Order!" VARCHAR(20),
                    "1st_shift" INTEGER
                )
                """
            )
        )
        conn.execute(text("INSERT INTO colliding_cols VALUES (1, 'A', 'B', 10), (2, 'C', 'D', 20)"))
    engine.dispose()

    res = discover_database(str(db_file))
    ddl = render_unity_catalog_ddl(res)
    assert "`order`" in ddl
    assert "`order_2`" in ddl
    assert "`c_1st_shift`" in ddl

    py_code = render_lakeflow_pipeline_py(res)
    ast.parse(py_code)
    assert "'order_2'" in py_code

    dbt_dir = tmp_path / "dbt"
    export_dbt_project(res, dbt_dir)
    stg_sql = (dbt_dir / "staging" / "stg_main__colliding_cols.sql").read_text(encoding="utf-8")
    assert "as order," in stg_sql
    assert "as order_2," in stg_sql
    assert "as c_1st_shift" in stg_sql

    # Multi-schema table name deduplication
    t1 = TableInfo(schema_name="erp", name="orders", full_name="erp.orders")
    t2 = TableInfo(schema_name="mes", name="orders", full_name="mes.orders")
    tmap = deduplicate_table_names([t1, t2])
    assert tmap["erp.orders"] == "erp_orders"
    assert tmap["mes.orders"] == "mes_orders"


def test_regression_6_html_has_no_inline_onclick_jump_handlers(tmp_path):
    """Bug #6: HTML report must use data-jump attributes instead of inline onclick JS string interpolation."""
    db_file = tmp_path / "quote_tbl.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE "plant\'s_line" (id INTEGER PRIMARY KEY, val TEXT)'))
        conn.execute(text('INSERT INTO "plant\'s_line" VALUES (1, \'ok\')'))
    engine.dispose()

    res = discover_database(str(db_file))
    html_text = render_html_report(res)
    assert "onclick=\"jumpToTable(" not in html_text
    assert 'data-jump="' in html_text
    assert 'closest("[data-jump]")' in html_text


def test_regression_7_pii_no_shopfloor_cell_false_positive_and_spaced_names():
    """Bug #7: Shop-floor 'work_cell'/'cell_id' must not trigger phone PII; 'First Name' must trigger person_name."""
    assert detect_pii("work_cell", "string", ["CELL-A", "CELL-B", "CELL-C"]) == []
    assert detect_pii("cell_id", "integer", ["1", "2", "3"]) == []

    # True cell phone column still detected
    cell_phone = detect_pii("cell_phone", "string", ["+1-555-234-5678", "+1-555-876-5432"])
    assert any(f.category == "phone" for f in cell_phone)

    # Spaced SQL Server column names detected
    fn = detect_pii("First Name", "string", ["Alice", "Bob"])
    assert any(f.category == "person_name" for f in fn)
    em = detect_pii("Email Address", "string", ["a@b.com", "c@d.org"])
    assert any(f.category == "email" for f in em)


def test_regression_8_sap_ddic_schema_qualified_and_preserves_semantic_name(tmp_path):
    """Bug #8: SAP DD02T/DD03T in non-default schema must load descriptions without overwriting semantic_name."""
    db_file = tmp_path / "sap_ddic.duckdb"
    engine = create_engine(f"duckdb:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA sapr3"))
        conn.execute(
            text("CREATE TABLE sapr3.DD02T (tabname VARCHAR, ddlanguage VARCHAR, ddtext VARCHAR)")
        )
        conn.execute(
            text("CREATE TABLE sapr3.DD03T (fieldname VARCHAR, ddlanguage VARCHAR, ddtext VARCHAR)")
        )
        conn.execute(text("CREATE TABLE sapr3.ZCUSTOM (id INTEGER PRIMARY KEY, zcol VARCHAR)"))
        conn.execute(
            text("INSERT INTO sapr3.DD02T VALUES ('ZCUSTOM', 'E', 'Custom Plant Schedule - Free Text')")
        )
        conn.execute(
            text("INSERT INTO sapr3.DD03T VALUES ('ZCOL', 'E', 'Custom Schedule Code')")
        )
        conn.execute(text("INSERT INTO sapr3.ZCUSTOM VALUES (1, 'A')"))
    engine.dispose()

    res = discover_database(str(db_file))
    zcustom = next(t for t in res.tables if t.name.upper() == "ZCUSTOM")
    assert zcustom.business_description == "Custom Plant Schedule - Free Text"
    # semantic_name must NOT be polluted with free-text ddtext
    assert zcustom.semantic_name is None
    zcol = next(c for c in zcustom.columns if c.name.upper() == "ZCOL")
    assert zcol.business_description == "Custom Schedule Code"


def test_regression_9_telemetry_prefers_sensor_timestamp_over_etl_timestamp(tmp_path):
    """Bug #9: OT telemetry detector must prefer 'recorded_at' over earlier ordinal 'ingested_at'."""
    db_file = tmp_path / "ot_ts.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE historian_events (
                    ingested_at TIMESTAMP NOT NULL,
                    tag_id VARCHAR(32) NOT NULL,
                    recorded_at TIMESTAMP NOT NULL,
                    value DOUBLE NOT NULL
                )
                """
            )
        )
        rows = [
            {
                "ing": now.isoformat(sep=" ", timespec="seconds"),  # constant batch ingest time!
                "tag": "TAG_1",
                "rec": (now - timedelta(seconds=i * 5)).isoformat(sep=" ", timespec="seconds"),
                "val": 100.0 + i * 0.5,
            }
            for i in range(30)
        ]
        conn.execute(
            text("INSERT INTO historian_events VALUES (:ing, :tag, :rec, :val)"),
            rows,
        )
    engine.dispose()

    res = discover_database(str(db_file))
    tbl = next(t for t in res.tables if t.name == "historian_events")
    assert tbl.telemetry_profile is not None
    assert tbl.telemetry_profile.timestamp_column == "recorded_at"
    assert tbl.telemetry_profile.median_interval_seconds == 5.0


def test_regression_10_governance_values_not_leaked_into_dlt_expectations():
    """Bug #10: Governance-sensitive columns (ITAR/commercial) must not emit literal values into @dlt.expect_all."""
    col = ColumnInfo(
        name="itar_class",
        raw_type="VARCHAR(20)",
        canonical_type="string",
        nullable=False,
        profile=ColumnProfile(
            sample_size=20,
            non_null_count=20,
            null_count=0,
            null_rate=0.0,
            distinct_count=2,
            cardinality_class="low",
            top_values=[
                ValueFrequency(value="SECRET_ITAR_A", count=10, rate=0.5),
                ValueFrequency(value="SECRET_ITAR_B", count=10, rate=0.5),
            ],
            governance_findings=[
                GovernanceFinding(
                    category="ip_or_export_controlled",
                    confidence="high",
                    uc_policy="column_mask",
                    uc_tag_key="compliance",
                    uc_tag_value="itar_ear_ip",
                )
            ],
        ),
    )
    tbl = TableInfo(schema_name="main", name="specs", full_name="main.specs", columns=[col])
    res = DiscoveryResult(
        config=DiscoveryConfig(),
        overview=DatabaseOverview(
            dialect="sqlite",
            driver="pysqlite",
            database_name="test",
            sanitized_url="sqlite:///:memory:",
            discovered_at="2026-11-27T00:00:00Z",
        ),
        tables=[tbl],
    )
    py_code = render_lakeflow_pipeline_py(res)
    ast.parse(py_code)
    assert "SECRET_ITAR_A" not in py_code
    assert "SECRET_ITAR_B" not in py_code


def test_regression_11_decimal_keys_verified_in_relationship_inference(tmp_path):
    """Bug #11: DECIMAL/NUMERIC key columns (e.g. Oracle NUMBER) must be cached for value-overlap join verification."""
    db_file = tmp_path / "decimal_fk.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE vendors (id DECIMAL(10, 2) PRIMARY KEY, name TEXT)"))
        conn.execute(text("CREATE TABLE invoices (id INTEGER PRIMARY KEY, vendor_id DECIMAL(10, 2), amount DECIMAL(10, 2))"))
        conn.execute(text("INSERT INTO vendors VALUES (10.50, 'V1'), (20.50, 'V2'), (30.50, 'V3')"))
        conn.execute(text("INSERT INTO invoices VALUES (1, 10.50, 100.0), (2, 20.50, 200.0), (3, 10.50, 300.0)"))
    engine.dispose()

    res = discover_database(str(db_file))
    inferred = [r for r in res.relationships if r.source_column == "vendor_id"]
    assert len(inferred) == 1
    assert inferred[0].kind == "inferred_name_and_values"
    assert inferred[0].overlap_rate == 1.0


def test_regression_12_harmonize_multi_schema_and_sql_escaping():
    """Bug #12: Multi-schema same-named tables in one plant must not overwrite each other; quotes/backticks escaped."""
    def _make_snap(db_name: str, tables: list[TableInfo]) -> DiscoveryResult:
        return DiscoveryResult(
            config=DiscoveryConfig(),
            overview=DatabaseOverview(
                dialect="mssql",
                driver="pyodbc",
                database_name=db_name,
                sanitized_url="mssql://test",
                discovered_at="2026-11-27T00:00:00Z",
            ),
            tables=tables,
        )

    s1 = _make_snap(
        "p1",
        [
            TableInfo(
                schema_name="erp",
                name="orders",
                full_name="erp.orders",
                columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer")],
            ),
            TableInfo(
                schema_name="mes",
                name="orders",
                full_name="mes.orders",
                columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer")],
            ),
        ],
    )
    s2 = _make_snap(
        "p2",
        [
            TableInfo(
                schema_name="erp",
                name="orders",
                full_name="erp.orders",
                columns=[ColumnInfo(name="id", raw_type="VARCHAR(20)", canonical_type="string")],
            ),
        ],
    )
    report = harmonize_snapshots({"plant's_one": s1, "plant_two": s2})
    names = {t.table_name for t in report.tables}
    assert "erp.orders" in names
    assert "mes.orders" in names
    erp_tbl = next(t for t in report.tables if t.table_name == "erp.orders")
    assert "'plant''s_one' AS `source_plant`" in erp_tbl.unified_sql
