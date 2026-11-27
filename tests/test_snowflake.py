"""Tests for Snowflake source adapter, type mapping, compatibility linter, exporters, harmonization, and CLI."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import yaml
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database, harmonize_snapshots
from tick.cli import app
from tick.demo import create_manufacturing_demo_database
from tick.dialects import SnowflakeAdapter, get_dialect_adapter
from tick.exporters import (
    export_snowflake_bundle,
    render_harmonization_html,
    render_html_report,
    render_odcs_yaml,
    render_snowflake_ddl,
    render_snowflake_dmf_sql,
    render_snowflake_dynamic_tables_sql,
    render_snowflake_sizing_md,
)
from tick.models import (
    ColumnInfo,
    ColumnProfile,
    DatabaseOverview,
    DiscoveryResult,
    GovernanceFinding,
    PiiFinding,
    TableInfo,
)
from tick.snowflake import (
    lint_snowflake_compatibility,
    map_to_snowflake_type,
    sanitize_snowflake_identifier,
)

runner = CliRunner()


def test_snowflake_adapter_safe_session_sampling_stats_and_rollback():
    sf = get_dialect_adapter("snowflake")
    assert isinstance(sf, SnowflakeAdapter)

    assert "SAMPLE (100 ROWS)" in sf.build_sample_query('"T"', '"ID"', 100, 5000, use_tablesample=True)
    assert "LIMIT 100" in sf.build_sample_query('"T"', '"ID"', 100, 150, use_tablesample=True)

    conn = MagicMock()
    sf.configure_safe_session(conn, read_uncommitted=True, query_timeout_seconds=30)
    executed_sql = str(conn.execute.call_args[0][0])
    assert "STATEMENT_TIMEOUT_IN_SECONDS = 30" in executed_sql

    audit = sf.audit_database_cdc(conn)
    assert audit.db_cdc_enabled is True
    assert any("Dynamic Tables" in n for n in audit.notes)

    # Error handling rolls back active transaction cleanly
    conn.reset_mock()
    conn.in_transaction.return_value = True
    conn.execute.side_effect = RuntimeError("warehouse suspended")
    assert sf.get_table_stats(conn, "PUBLIC") == {}
    assert conn.rollback.call_count == 1


def test_snowflake_type_mapping_and_linter():
    assert sanitize_snowflake_identifier("Order #") == "ORDER"
    assert sanitize_snowflake_identifier("1st_shift") == "C_1ST_SHIFT"

    assert map_to_snowflake_type("DECIMAL(50, 4)", "decimal") == "NUMBER(38, 4)"
    assert map_to_snowflake_type("DECIMAL(18, 0)", "integer") == "NUMBER(18, 0)"
    assert map_to_snowflake_type("MONEY", "decimal") == "NUMBER(19, 4)"
    assert map_to_snowflake_type("TIME", "time") == "TIME"
    assert map_to_snowflake_type("DATETIME2", "timestamp") == "TIMESTAMP_NTZ"
    assert map_to_snowflake_type("TIMESTAMPTZ", "timestamp") == "TIMESTAMP_LTZ"
    assert map_to_snowflake_type("JSONB", "json") == "VARIANT"
    assert map_to_snowflake_type("GEOGRAPHY", "other") == "GEOGRAPHY"
    assert map_to_snowflake_type("GEOMETRY", "other") == "GEOMETRY"
    assert map_to_snowflake_type("SQL_VARIANT", "other") == "VARCHAR"

    tbl = TableInfo(
        schema_name="main",
        name="erp_mix",
        full_name="main.erp_mix",
        columns=[
            ColumnInfo(name="Order #", raw_type="INT", canonical_type="integer"),
            ColumnInfo(name="huge_num", raw_type="DECIMAL(42, 0)", canonical_type="decimal"),
            ColumnInfo(name="exotic", raw_type="SQL_VARIANT", canonical_type="other"),
            ColumnInfo(name="geo_loc", raw_type="GEOGRAPHY", canonical_type="other"),
            ColumnInfo(name="payload", raw_type="JSONB", canonical_type="json"),
            ColumnInfo(name="created_at", raw_type="TIMESTAMP", canonical_type="timestamp"),
        ],
    )
    lint_snowflake_compatibility([tbl])
    assert tbl.snowflake_compatibility is not None
    assert tbl.snowflake_compatibility.requires_quoted_identifiers is True
    assert tbl.snowflake_compatibility.is_compatible is False
    cats = {(i.column, i.category) for i in tbl.snowflake_compatibility.issues}
    assert ("Order #", "quoted_identifier") in cats
    assert ("huge_num", "decimal_precision_overflow") in cats
    assert ("exotic", "unsupported_type") in cats
    assert ("geo_loc", "unsupported_type") not in cats  # Snowflake supports GEOGRAPHY natively!
    assert ("payload", "variant_size_limit") in cats
    assert ("created_at", "timezone_naive_timestamp") in cats


def test_snowflake_bundle_end_to_end_and_policy_type_matching(tmp_path):
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    res = discover_database(
        str(db_path),
        config=DiscoveryConfig(
            target_platform="snowflake",
            snowflake_database="MFG_PROD",
            snowflake_schema="BRONZE_OHIO",
            snowflake_warehouse="ETL_WH",
            skip_empty_tables=True,
        ),
    )

    assert res.overview.snowflake_warning_count >= 2
    # When target_platform="snowflake", Delta linter does not run
    assert all(t.delta_compatibility is None for t in res.tables)
    assert all(t.snowflake_compatibility is not None for t in res.tables)

    sf_dir = tmp_path / "snowflake_out"
    written = export_snowflake_bundle(res, sf_dir)
    assert len(written) == 7

    ddl = (sf_dir / "snowflake_ddl.sql").read_text(encoding="utf-8")
    assert 'CREATE DATABASE IF NOT EXISTS "MFG_PROD";' in ddl
    assert 'CREATE SCHEMA IF NOT EXISTS "MFG_PROD"."BRONZE_OHIO";' in ddl
    assert "CHANGE_TRACKING = TRUE" in ddl
    assert 'CREATE TAG IF NOT EXISTS "MFG_PROD"."BRONZE_OHIO"."COMPLIANCE";' in ddl
    assert "CREATE OR REPLACE ROW ACCESS POLICY" in ddl
    # Type-matched masking policies: VARCHAR + NUMBER(12, 2) for unit_cost / STPRS!
    assert "MASK_SENSITIVE_VARCHAR" in ddl
    assert "MASK_SENSITIVE_NUMBER_12_2" in ddl
    assert "AS (VAL NUMBER(12, 2)) RETURNS NUMBER(12, 2)" in ddl

    dt_sql = (sf_dir / "snowflake_dynamic_tables.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE DYNAMIC TABLE" in dt_sql
    assert 'WAREHOUSE = "ETL_WH"' in dt_sql
    assert "TARGET_LAG = '5 minutes'" in dt_sql
    assert "TRY_TO_TIMESTAMP_NTZ(" in dt_sql
    assert 'WHERE "QUALITY_CODE" >= 192' in dt_sql
    assert "QUALIFY ROW_NUMBER() OVER" in dt_sql
    assert "WITH RECURSIVE" in dt_sql

    dmf_sql = (sf_dir / "snowflake_dmf.sql").read_text(encoding="utf-8")
    assert "DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'" in dmf_sql
    assert "SNOWFLAKE.CORE.ROW_COUNT" in dmf_sql
    assert "SNOWFLAKE.CORE.DUPLICATE_COUNT" in dmf_sql
    assert "SNOWFLAKE.CORE.NULL_COUNT" in dmf_sql
    assert "SNOWFLAKE.CORE.FRESHNESS" in dmf_sql
    assert "BAD_OPC_QUALITY_COUNT" in dmf_sql
    assert "ARG_C < 192" in dmf_sql

    sizing_md = (sf_dir / "snowflake_sizing.md").read_text(encoding="utf-8")
    assert "Snowflake Ingestion, Dynamic Tables & Sizing Plan" in sizing_md
    assert "snowpipe_streaming" in sizing_md

    # ODCS targets Snowflake server cleanly
    odcs = yaml.safe_load(render_odcs_yaml(res))
    server_types = {s["type"] for s in odcs["servers"]}
    assert "snowflake" in server_types
    assert "databricks" not in server_types
    sf_srv = next(s for s in odcs["servers"] if s["type"] == "snowflake")
    assert sf_srv["database"] == "MFG_PROD"
    assert sf_srv["warehouse"] == "ETL_WH"

    # Discovery HTML embeds Snowflake code tabs & linter table
    html_text = render_html_report(res)
    assert "Snowflake DDL &amp; Policies" in html_text
    assert "Snowflake Dynamic Tables" in html_text
    assert "Snowflake DMFs" in html_text
    assert 'id="sf-lint-table"' in html_text


def test_snowflake_row_access_policy_dedup_and_integer_plant_key():
    tbl = TableInfo(
        schema_name="main",
        name="ledger",
        full_name="main.ledger",
        columns=[
            ColumnInfo(
                name="plant_id",
                raw_type="INTEGER",
                canonical_type="integer",
                snowflake_type="INTEGER",
                profile=ColumnProfile(
                    governance_findings=[
                        GovernanceFinding(
                            category="plant_isolation_key",
                            confidence="high",
                            uc_policy="row_filter",
                            uc_tag_key="governance_scope",
                            uc_tag_value="plant_isolation",
                        )
                    ]
                ),
            ),
            ColumnInfo(
                name="WERKS",
                raw_type="VARCHAR",
                canonical_type="string",
                snowflake_type="VARCHAR",
                profile=ColumnProfile(
                    governance_findings=[
                        GovernanceFinding(
                            category="plant_isolation_key",
                            confidence="high",
                            uc_policy="row_filter",
                            uc_tag_key="governance_scope",
                            uc_tag_value="plant_isolation",
                        )
                    ]
                ),
            ),
            ColumnInfo(
                name="email",
                raw_type="VARCHAR",
                canonical_type="string",
                snowflake_type="VARCHAR",
                profile=ColumnProfile(
                    pii_findings=[PiiFinding(category="email", confidence="high", score=0.95)],
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
            ),
        ],
    )
    res = DiscoveryResult(
        config=DiscoveryConfig(target_platform="snowflake"),
        overview=DatabaseOverview(
            dialect="duckdb",
            driver="duckdb_engine",
            database_name="test",
            sanitized_url="duckdb:///test",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=1,
            view_count=0,
            column_count=3,
        ),
        tables=[tbl],
    )
    ddl = render_snowflake_ddl(res)
    assert "PLANT_ROW_ACCESS_INTEGER" in ddl
    assert "AS (PLANT_VAL INTEGER) RETURNS BOOLEAN" in ddl
    assert ddl.count("ADD ROW ACCESS POLICY") == 1
    assert ddl.count('MODIFY COLUMN "EMAIL" SET MASKING POLICY') == 1


def test_snowflake_multi_plant_harmonization_and_cli(tmp_path):
    ohio_db = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    munich_db = create_manufacturing_demo_database(tmp_path / "munich.duckdb", plant_variant="plant_munich")

    ohio_out = tmp_path / "ohio_bundle"
    munich_out = tmp_path / "munich_bundle"

    r1 = runner.invoke(
        app,
        [
            "discover",
            str(ohio_db),
            "--out-dir",
            str(ohio_out),
            "--target",
            "both",
            "--snowflake-db",
            "MFG_DW",
            "--quiet",
        ],
    )
    assert r1.exit_code == 0
    assert (ohio_out / "databricks" / "unity_catalog_ddl.sql").exists()
    assert (ohio_out / "snowflake" / "snowflake_ddl.sql").exists()
    assert (ohio_out / "snowflake" / "snowflake_dynamic_tables.sql").exists()
    assert (ohio_out / "snowflake" / "snowflake_dmf.sql").exists()

    r2 = runner.invoke(
        app,
        ["discover", str(munich_db), "--json", str(munich_out / "snapshot.json"), "--quiet"],
    )
    assert r2.exit_code == 0

    sf_sql = tmp_path / "unified_sf.sql"
    sf_html = tmp_path / "harm_sf.html"
    sf_json = tmp_path / "harm_sf.json"
    r3 = runner.invoke(
        app,
        [
            "compare-sources",
            str(ohio_out / "snapshot.json"),
            str(munich_out / "snapshot.json"),
            "--target",
            "snowflake",
            "--database",
            "MFG_DW",
            "--sql",
            str(sf_sql),
            "--html",
            str(sf_html),
            "--json",
            str(sf_json),
        ],
    )
    assert r3.exit_code == 0
    sql_text = sf_sql.read_text(encoding="utf-8")
    assert 'CREATE OR REPLACE VIEW "MFG_DW"."SILVER"."UNIFIED_WORK_ORDERS" AS' in sql_text
    assert "'ohio_bundle' AS \"SOURCE_PLANT\"" in sql_text
    assert 'CAST("Order #" AS VARCHAR) AS "ORDER"' in sql_text
    assert 'CAST(NULL AS VARCHAR) AS "EU_SHIFT_SUPERVISOR"' in sql_text
    assert "`" not in sql_text

    data = json.loads(sf_json.read_text(encoding="utf-8"))
    assert data["target_platform"] == "snowflake"

    html_text = sf_html.read_text(encoding="utf-8")
    assert "Snowflake" in html_text
    assert "tick-harmonize-data" in html_text


def test_snowflake_existing_audit_cols_dynamic_tags_and_target_isolation(tmp_path):
    tbl = TableInfo(
        schema_name="bronze",
        name="events",
        full_name="bronze.events",
        columns=[
            ColumnInfo(
                name="id",
                raw_type="BIGINT",
                canonical_type="integer",
                snowflake_type="BIGINT",
                is_primary_key=True,
                nullable=False,
            ),
            ColumnInfo(
                name="_ingested_at",
                raw_type="TIMESTAMP",
                canonical_type="timestamp",
                snowflake_type="TIMESTAMP_LTZ",
            ),
            ColumnInfo(
                name="_source_system",
                raw_type="VARCHAR",
                canonical_type="string",
                snowflake_type="VARCHAR",
                profile=ColumnProfile(
                    governance_findings=[
                        GovernanceFinding(
                            category="commercial_sensitive",
                            confidence="high",
                            uc_policy="uc_tag",
                            uc_tag_key="custom_domain_tag",
                            uc_tag_value="plant_a",
                        )
                    ]
                ),
            ),
        ],
        primary_key=["id"],
    )
    res = DiscoveryResult(
        config=DiscoveryConfig(target_platform="snowflake"),
        overview=DatabaseOverview(
            dialect="snowflake",
            driver="snowflake",
            database_name="PROD_DW",
            sanitized_url="snowflake://user@acct/PROD_DW",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=1,
            view_count=0,
            column_count=3,
        ),
        tables=[tbl],
    )
    ddl = render_snowflake_ddl(res, database="CUSTOM_DB", schema="CUSTOM_SCH")
    assert ddl.count('"_INGESTED_AT"') == 1
    assert ddl.count('"_SOURCE_SYSTEM"') == 1
    assert 'CREATE TAG IF NOT EXISTS "CUSTOM_DB"."CUSTOM_SCH"."CUSTOM_DOMAIN_TAG";' in ddl

    dt_sql = render_snowflake_dynamic_tables_sql(res, database="CUSTOM_DB", bronze_schema="CUSTOM_SCH")
    assert dt_sql.count('"_INGESTED_AT"') == 1
    assert dt_sql.count('"_SOURCE_SYSTEM"') == 1

    sizing = render_snowflake_sizing_md(res, database="CUSTOM_DB", schema="CUSTOM_SCH", warehouse="BIG_WH")
    assert "`CUSTOM_DB.CUSTOM_SCH`" in sizing
    assert "`BIG_WH`" in sizing

    # target_platform="databricks" omits snowflake_target server from ODCS & skips snowflake lint
    db_path = create_manufacturing_demo_database(tmp_path / "dbx_only.duckdb", plant_variant="plant_ohio")
    dbx_res = discover_database(str(db_path), config=DiscoveryConfig(target_platform="databricks"))
    assert all(t.snowflake_compatibility is None for t in dbx_res.tables)
    odcs_dbx = yaml.safe_load(render_odcs_yaml(dbx_res))
    assert {s["server"] for s in odcs_dbx["servers"]} == {"source", "databricks_target"}

