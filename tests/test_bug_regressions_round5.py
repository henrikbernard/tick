"""Regression tests for Round-5 code review bug fixes (security, Snowflake metadata, SQL, ODCS, HTML, CLI)."""

from __future__ import annotations

from unittest.mock import MagicMock

import yaml
from sqlalchemy.engine.url import make_url
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database, harmonize_snapshots
from tick.cli import app
from tick.databricks import map_to_spark_type
from tick.demo import create_manufacturing_demo_database
from tick.dialects import BigQueryAdapter, SnowflakeAdapter, sanitize_url
from tick.exporters import (
    export_contracts_bundle,
    render_databricks_sizing_md,
    render_harmonization_html,
    render_html_report,
    render_markdown_report,
    render_odcs_yaml,
    render_snowflake_ddl,
    render_unity_catalog_ddl,
)
from tick.harmonize import _widen_snowflake_types
from tick.models import (
    ColumnInfo,
    ColumnProfile,
    DatabaseOverview,
    DiscoveryResult,
    GovernanceFinding,
    PiiFinding,
    TableInfo,
)
from tick.profiler import _fetch_full_table_aggregates

runner = CliRunner()


def test_sanitize_url_masks_snowflake_keypair_mfa_and_oauth_secrets():
    url = (
        "snowflake://svc_etl:rawpass@acct.us-east-1/MFG_DW/BRONZE"
        "?private_key_file_pwd=keysecret123&passcode=998877&oauth_token=tok_abc&warehouse=ETL_WH"
    )
    sanitized = sanitize_url(url)
    for secret in ("rawpass", "keysecret123", "998877", "tok_abc"):
        assert secret not in sanitized
    assert "warehouse=ETL_WH" in sanitized

    # Fallback regex path (unparseable URL) also masks Snowflake secrets
    malformed = "not a url://u:p@h?private_key_file_pwd=s1&private_key=s2&passcode=s3&oauth_token=s4&ok=1"
    sanitized_fb = sanitize_url(malformed)
    for secret in ("s1", "s2", "s3", "s4"):
        assert secret not in sanitized_fb
    assert "ok=1" in sanitized_fb


def test_snowflake_and_bigquery_get_database_name_parses_db_not_schema():
    sf = SnowflakeAdapter()
    eng_sf = MagicMock()
    eng_sf.url = make_url("snowflake://user:pass@acct/PROD_WAREHOUSE/PUBLIC")
    eng_sf.dialect.name = "snowflake"
    assert sf.get_database_name(eng_sf) == "PROD_WAREHOUSE"

    eng_sf_q = MagicMock()
    eng_sf_q.url = make_url("snowflake://user:pass@acct/?database=QUERY_DB")
    eng_sf_q.dialect.name = "snowflake"
    assert sf.get_database_name(eng_sf_q) == "QUERY_DB"

    bq = BigQueryAdapter()
    eng_bq = MagicMock()
    eng_bq.url = make_url("bigquery://my-gcp-project/analytics_ds")
    eng_bq.dialect.name = "bigquery"
    assert bq.get_database_name(eng_bq) == "my-gcp-project.analytics_ds"


def test_snowflake_get_table_stats_handles_lowercase_row_keys_and_upper_schema():
    sf = SnowflakeAdapter()
    conn = MagicMock()
    # Real snowflake-sqlalchemy often returns lowercase RowMapping keys while filtering UPPER(TABLE_SCHEMA)
    conn.execute.return_value.mappings.return_value = [
        {
            "table_name": "ORDERS",
            "table_type": "BASE TABLE",
            "row_count": 42000,
            "bytes": 1048576,
            "comment": "Customer orders",
        }
    ]
    stats = sf.get_table_stats(conn, "public")
    executed_sql = str(conn.execute.call_args.args[0])
    assert "UPPER(TABLE_SCHEMA) = UPPER(:schema)" in executed_sql
    assert "ORDERS" in stats
    assert stats["ORDERS"].row_count == 42000
    assert stats["ORDERS"].size_bytes == 1048576


def test_profiler_full_table_aggregates_handles_uppercase_folded_aliases():
    """Snowflake, Oracle, and DB2 fold unquoted aliases (t_total -> T_TOTAL)."""
    conn = MagicMock()
    conn.exec_driver_sql.return_value.mappings.return_value.first.return_value = {
        "T_TOTAL": 500,
        "C_NN_0": 495,
        "C_MIN_0": 1,
        "C_MAX_0": 999,
    }
    adapter = MagicMock()
    adapter.name = "snowflake"
    adapter.quote_identifier.side_effect = lambda _c, name: f'"{name}"'
    cols = [ColumnInfo(name="amount", raw_type="NUMBER(10,0)", canonical_type="integer")]
    aggs = _fetch_full_table_aggregates(conn, adapter, '"PUBLIC"."ORDERS"', cols)
    assert aggs["amount"] == {"total": 500, "non_null": 495, "min": 1, "max": 999}


def test_snowflake_timestamp_tz_maps_to_spark_timestamp_and_widens_cleanly():
    assert map_to_spark_type("TIMESTAMP_TZ", "timestamp") == "TIMESTAMP"
    widened, conflict = _widen_snowflake_types(["DATE", "TIMESTAMP_TZ"])
    assert widened == "TIMESTAMP_TZ"
    assert conflict is True


def test_harmonize_avoids_source_plant_column_and_view_name_collisions():
    def _make_res(db_name: str, extra_table_name: str) -> DiscoveryResult:
        return DiscoveryResult(
            config=DiscoveryConfig(),
            overview=DatabaseOverview(
                dialect="duckdb",
                driver="duckdb_engine",
                database_name=db_name,
                sanitized_url=f"duckdb:///{db_name}",
                discovered_at="2026-01-01T00:00:00Z",
                duration_seconds=0.1,
                schema_count=1,
                table_count=2,
                view_count=0,
                column_count=4,
            ),
            tables=[
                TableInfo(
                    schema_name="main",
                    name="order_items",
                    full_name="main.order_items",
                    columns=[
                        ColumnInfo(name="id", raw_type="INTEGER", canonical_type="integer", is_primary_key=True),
                        ColumnInfo(name="source_plant", raw_type="VARCHAR", canonical_type="string"),
                    ],
                    primary_key=["id"],
                ),
                TableInfo(
                    schema_name="main",
                    name=extra_table_name,
                    full_name=f"main.{extra_table_name}",
                    columns=[ColumnInfo(name="id", raw_type="INTEGER", canonical_type="integer")],
                ),
            ],
        )

    sources = {"ohio": _make_res("ohio", "order-items"), "munich": _make_res("munich", "order-items")}

    for plat in ("databricks", "snowflake"):
        rep = harmonize_snapshots(sources, target_platform=plat)  # type: ignore[arg-type]
        sql_by_tbl = {t.table_name: t.unified_sql for t in rep.tables}
        # Synthetic plant discriminator must NOT collide with existing `source_plant` column
        oi_sql = sql_by_tbl["order_items"]
        if plat == "snowflake":
            assert 'AS "_TICK_SOURCE_PLANT"' in oi_sql
            assert '"source_plant" AS "SOURCE_PLANT"' in oi_sql
            # Distinct view names for `order-items` vs `order_items`
            assert '"UNIFIED_ORDER_ITEMS"' in sql_by_tbl["order-items"]
            assert '"UNIFIED_ORDER_ITEMS_2"' in oi_sql
        else:
            assert "AS `_tick_source_plant`" in oi_sql
            assert "`source_plant` AS `source_plant`" in oi_sql
            assert "`unified_order_items`" in sql_by_tbl["order-items"]
            assert "`unified_order_items_2`" in oi_sql


def test_odcs_snowflake_physical_types_domain_and_clean_contracts_bundle(tmp_path):
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    res = discover_database(
        str(db_path),
        config=DiscoveryConfig(
            target_platform="snowflake",
            snowflake_database="ACME_SF_DW",
            snowflake_schema="RAW_OHIO",
        ),
    )
    odcs = yaml.safe_load(render_odcs_yaml(res))
    assert odcs["domain"] == "ACME_SF_DW"
    schema_by_name = {s["name"]: s for s in odcs["schema"]}
    wo_props = {p["name"]: p for p in schema_by_name["WORK_ORDERS"]["properties"]}
    # Snowflake physical types (INTEGER / NUMBER) & uppercase Snowflake DDL identifiers
    assert wo_props["ORDER"]["physicalType"] == "INTEGER"
    assert wo_props["UNIT_COST"]["physicalType"].startswith("NUMBER(")
    assert wo_props["PLANT_CODE"]["physicalType"] == "VARCHAR"

    # Snowflake-only bundle should NOT emit Databricks-only dqx_checks.yml
    written = export_contracts_bundle(res, tmp_path / "sf_contracts")
    names = {p.name for p in written}
    assert names == {"data_contract.odcs.yml"}


def test_databricks_and_snowflake_ddl_no_duplicate_audit_cols_and_no_25_mask_cap():
    cols = [
        ColumnInfo(name="id", raw_type="BIGINT", canonical_type="integer", is_primary_key=True, nullable=False),
        ColumnInfo(name="_ingested_at", raw_type="TIMESTAMP", canonical_type="timestamp"),
        ColumnInfo(name="_source_system", raw_type="VARCHAR", canonical_type="string"),
    ]
    # Add 30 PII columns to ensure masking policies are NOT silently truncated at 25
    for i in range(30):
        cols.append(
            ColumnInfo(
                name=f"pii_field_{i}",
                raw_type="VARCHAR",
                canonical_type="string",
                profile=ColumnProfile(
                    pii_findings=[PiiFinding(category="email", confidence="high", score=0.95)],
                    governance_findings=[
                        GovernanceFinding(
                            category="commercial_sensitive",
                            confidence="high",
                            uc_policy="uc_tag",
                            uc_tag_key="team's_tag",
                            uc_tag_value="plant's_val",
                        )
                    ]
                    if i == 0
                    else [],
                ),
            )
        )
    res = DiscoveryResult(
        config=DiscoveryConfig(target_platform="both"),
        overview=DatabaseOverview(
            dialect="duckdb",
            driver="duckdb_engine",
            database_name="big_erp",
            sanitized_url="duckdb:///big_erp",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=1,
            view_count=0,
            column_count=len(cols),
        ),
        tables=[TableInfo(schema_name="main", name="wide_pii", full_name="main.wide_pii", columns=cols, primary_key=["id"])],
    )
    dbx_ddl = render_unity_catalog_ddl(res, catalog="cat_x", schema="sch_x")
    assert dbx_ddl.count("`_ingested_at`") == 1
    assert dbx_ddl.count("`_source_system`") == 1
    assert "SET MASK" in dbx_ddl
    assert dbx_ddl.count("SET MASK") == 30
    assert r"'team\'s_tag' = 'plant\'s_val'" in dbx_ddl

    sf_ddl = render_snowflake_ddl(res, database="DB_X", schema="SCH_X")
    assert sf_ddl.count("SET MASKING POLICY") == 30

    dbx_sizing = render_databricks_sizing_md(res, catalog="cat_override", schema="sch_override")
    assert "`cat_override.sch_override`" in dbx_sizing


def test_snowflake_html_markdown_and_cli_surface_snowflake_metadata(tmp_path):
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    res = discover_database(str(db_path), config=DiscoveryConfig(target_platform="snowflake"))

    html_text = render_html_report(res)
    assert "hasHighSf" in html_text
    assert "Snowflake Clustering &amp; Warehouse" in html_text
    assert 'targetPlatform === "snowflake" ? "sf_ddl" : "uc_ddl"' in html_text

    md_text = render_markdown_report(res)
    assert "Delta / Snowflake Compatibility Warnings" in md_text
    assert "Snowflake Clustering & Warehouse" in md_text
    assert "SF:`" in md_text

    rep = harmonize_snapshots({"ohio": res, "ohio2": res}, target_platform="snowflake")
    harm_html = render_harmonization_html(rep)
    assert "Auto-widened in Snowflake SQL" in harm_html
    assert "unified_snowflake_views.sql" in harm_html

    # Non-quiet CLI with --target snowflake renders Snowflake summary cleanly
    cli_res = runner.invoke(
        app,
        ["discover", str(db_path), "--target", "snowflake"],
        env={"COLUMNS": "180"},
    )
    assert cli_res.exit_code == 0
    assert "Snowflake Blueprint" in cli_res.output
    assert "SF Warns" in cli_res.output
    assert "XSMALL" in cli_res.output
    assert "SF_QUOTE" in cli_res.output
