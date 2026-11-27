"""Regression tests for Round-6 code review bug fixes."""

from __future__ import annotations

import ast
import json

import sqlglot
import yaml

from tick import DiscoveryConfig, harmonize_snapshots
from tick.dialects import PostgresAdapter
from tick.exporters import (
    render_databricks_bundle_yml,
    render_databricks_gold_hierarchies_sql,
    render_harmonization_html,
    render_html_report,
    render_lakeview_dashboard_json,
    render_snowflake_gold_hierarchies_sql,
    render_snowflake_streamlit_app,
)
from tick.models import (
    ColumnInfo,
    DatabaseOverview,
    DiscoveryResult,
    ForeignKeyInfo,
    HierarchyInfo,
    TableInfo,
    TelemetryProfile,
)
from tick.pii import detect_pii
from tick.relationships import discover_relationships, discover_view_lineage


def _make_base_result(tables: list[TableInfo], db_name: str = "test_db") -> DiscoveryResult:
    return DiscoveryResult(
        config=DiscoveryConfig(),
        overview=DatabaseOverview(
            dialect="postgresql",
            driver="psycopg",
            database_name=db_name,
            sanitized_url=f"postgresql://host/{db_name}",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=sum(1 for t in tables if t.table_type == "table"),
            view_count=sum(1 for t in tables if t.table_type != "table"),
            column_count=sum(len(t.columns) for t in tables),
        ),
        tables=tables,
    )


def test_gold_hierarchy_sql_handles_nullable_parent_trees_and_invalid_max_depth():
    tbl = TableInfo(
        schema_name="main",
        name="assets",
        full_name="main.assets",
        columns=[
            ColumnInfo(name="asset_id", raw_type="INTEGER", canonical_type="integer", is_primary_key=True, nullable=False),
            ColumnInfo(name="parent_asset_id", raw_type="INTEGER", canonical_type="integer", nullable=True),
        ],
        primary_key=["asset_id"],
        hierarchy=HierarchyInfo(
            pattern="asset_or_org_tree",
            parent_column="parent_asset_id",
            child_column="asset_id",
            reason="Self-referential asset tree",
        ),
    )
    res = _make_base_result([tbl])

    dbx_sql = render_databricks_gold_hierarchies_sql(res, max_depth=0)
    assert "COALESCE(e.`parent_asset_id`, e.`asset_id`) AS `root_node_id`" in dbx_sql
    assert "WHERE e.`parent_asset_id` IS NULL OR NOT EXISTS (" in dbx_sql
    assert "WHERE e.`parent_asset_id` IS NOT NULL" in dbx_sql
    assert "AND r.`depth` < 1" in dbx_sql
    assert len([s for s in sqlglot.parse(dbx_sql, read="databricks") if s is not None]) >= 2

    sf_sql = render_snowflake_gold_hierarchies_sql(res, max_depth=-5)
    assert 'COALESCE(e."PARENT_ASSET_ID", e."ASSET_ID") AS "ROOT_NODE_ID"' in sf_sql
    assert 'IFF(e."PARENT_ASSET_ID" IS NULL,' in sf_sql
    assert 'WHERE e."PARENT_ASSET_ID" IS NULL OR NOT EXISTS (' in sf_sql
    assert 'WHERE e."PARENT_ASSET_ID" IS NOT NULL' in sf_sql
    assert 'AND r."DEPTH" < 1' in sf_sql
    assert len([s for s in sqlglot.parse(sf_sql, read="snowflake") if s is not None]) >= 2


def test_streamlit_triple_quote_safety_db_qualified_dt_history_and_lakeview_gold_inventory():
    ot_tbl = TableInfo(
        schema_name="main",
        name="sensor_tags",
        full_name="main.sensor_tags",
        columns=[
            ColumnInfo(name="tag_id", raw_type="VARCHAR", canonical_type="string"),
            ColumnInfo(name="recorded_at", raw_type="TIMESTAMP", canonical_type="timestamp"),
            ColumnInfo(name="value", raw_type="DOUBLE", canonical_type="float"),
        ],
        telemetry_profile=TelemetryProfile(
            pattern="narrow_historian_eav",
            tag_column="tag_id",
            timestamp_column="recorded_at",
            value_columns=["value"],
        ),
    )
    bom_tbl = TableInfo(
        schema_name="main",
        name="bom",
        full_name="main.bom",
        columns=[
            ColumnInfo(name="parent_part_id", raw_type="VARCHAR", canonical_type="string"),
            ColumnInfo(name="child_part_id", raw_type="VARCHAR", canonical_type="string"),
        ],
        hierarchy=HierarchyInfo(
            pattern="bom",
            parent_column="parent_part_id",
            child_column="child_part_id",
            reason="BOM",
        ),
    )
    res = _make_base_result([ot_tbl, bom_tbl], db_name="plant/a:test")

    # Triple single-quotes in database/schema name must NOT break Streamlit Python syntax!
    sis_code = render_snowflake_streamlit_app(res, database="DB'''QUOTE'", bronze_schema="RAW'''1'")
    ast.parse(sis_code)
    assert 'FROM TABLE("{_DB_DQ}".INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY' in sis_code

    # Lakeview catalog inventory includes gold schema when hierarchies exist
    dash = json.loads(render_lakeview_dashboard_json(res, catalog="mfg", bronze_schema="bronze", gold_schema="gold"))
    inv = next(d for d in dash["datasets"] if d["name"] == "uc_catalog_inventory")
    assert "'gold'" in inv["query"]

    # DABs pipeline includes bronze_catalog / bronze_schema configuration
    dab = yaml.safe_load(render_databricks_bundle_yml(res, catalog="mfg", bronze_schema="bronze"))
    pipe = next(iter(dab["resources"]["pipelines"].values()))
    assert pipe["configuration"]["bronze_catalog"] == "${var.catalog}"
    assert pipe["configuration"]["bronze_schema"] == "${var.bronze_schema}"

    # Discovery HTML sanitizes DB name for SVG download filenames
    html_text = render_html_report(res)
    assert "safeDbFile" in html_text


def test_glossary_harmonization_preserves_snapshot_semantic_name_and_never_drops_collisions():
    # Plant A discovered with a custom semantic_name overriding built-in AUFK -> custom_shop_orders,
    # AND has a second table in the same schema mapping to the same canonical name!
    plant_a = _make_base_result(
        [
            TableInfo(
                schema_name="dbo",
                name="AUFK",
                full_name="dbo.AUFK",
                semantic_name="custom_shop_orders",
                columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer")],
            ),
            TableInfo(
                schema_name="dbo",
                name="AUFK_ARCHIVE",
                full_name="dbo.AUFK_ARCHIVE",
                semantic_name="custom_shop_orders",
                columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer")],
            ),
        ],
        db_name="plant_a",
    )
    plant_b = _make_base_result(
        [
            TableInfo(
                schema_name="dbo",
                name="custom_shop_orders",
                full_name="dbo.custom_shop_orders",
                columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer")],
            )
        ],
        db_name="plant_b",
    )

    rep = harmonize_snapshots({"plant_a": plant_a, "plant_b": plant_b}, match_by_glossary=True)
    # Neither table from plant_a is dropped; AUFK unified with plant_b under snapshot's custom_shop_orders (not built-in production_order_master!)
    tbl_names = [t.table_name for t in rep.tables]
    assert any("custom_shop_orders" in n for n in tbl_names)
    assert "production_order_master" not in tbl_names
    assert len(rep.tables) == 2  # AUFK + custom_shop_orders matched, AUFK_ARCHIVE preserved separately!

    # Harmonization HTML includes source_table_names/source_column_names in search + glossary filter pill
    h_html = render_harmonization_html(rep)
    assert 'data-filter="glossary"' in h_html
    assert "source_table_names" in h_html
    assert "source_column_names" in h_html


def test_view_lineage_does_not_mislink_schema_qualified_or_ambiguous_tables():
    tables = [
        TableInfo(schema_name="staging", name="orders", full_name="staging.orders", columns=[]),
        TableInfo(schema_name="archive", name="orders", full_name="archive.orders", columns=[]),
        TableInfo(
            schema_name="reporting",
            name="v_external_erp",
            full_name="reporting.v_external_erp",
            table_type="view",
            view_definition="CREATE VIEW reporting.v_external_erp AS SELECT * FROM erp.orders",
            columns=[],
        ),
        TableInfo(
            schema_name="reporting",
            name="v_ambiguous",
            full_name="reporting.v_ambiguous",
            table_type="view",
            view_definition="CREATE VIEW reporting.v_ambiguous AS SELECT * FROM orders",
            columns=[],
        ),
    ]
    discover_view_lineage(tables, PostgresAdapter())
    v_ext = next(t for t in tables if t.name == "v_external_erp")
    v_amb = next(t for t in tables if t.name == "v_ambiguous")
    # Explicitly schema-qualified `erp.orders` must NOT falsely bind to `staging.orders`
    assert v_ext.upstream_tables == ["erp.orders"]
    # Ambiguous unqualified `orders` present in 2 other schemas must NOT pick an arbitrary schema
    assert v_amb.upstream_tables == ["orders"]


def test_explicit_fk_case_insensitive_multi_schema_resolution():
    tables = [
        TableInfo(
            schema_name="public",
            name="customers",
            full_name="public.customers",
            columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
            primary_key=["id"],
        ),
        TableInfo(
            schema_name="staging",
            name="customers",
            full_name="staging.customers",
            columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
            primary_key=["id"],
        ),
        TableInfo(
            schema_name="public",
            name="orders",
            full_name="public.orders",
            columns=[ColumnInfo(name="customer_id", raw_type="INT", canonical_type="integer")],
            foreign_keys=[
                ForeignKeyInfo(
                    constrained_columns=["customer_id"],
                    referred_schema="PUBLIC",
                    referred_table="CUSTOMERS",
                    referred_columns=["id"],
                )
            ],
        ),
    ]
    rels, _ = discover_relationships(tables, {}, DiscoveryConfig(infer_relationships=False))
    assert len(rels) == 1
    assert rels[0].target_table == "public.customers"


def test_pii_detector_does_not_flag_boolean_columns_as_pii():
    assert detect_pii("email_opt_in", "boolean", ["true", "false"]) == []
    assert detect_pii("is_phone_verified", "boolean", ["true", "false"]) == []
    assert detect_pii("has_dob", "boolean", []) == []
    assert detect_pii("ssn_verified", "boolean", ["false", "true"]) == []
