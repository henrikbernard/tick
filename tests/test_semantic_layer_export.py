"""Tests for AI / Semantic Layer exporters: Snowflake Cortex Analyst YAML, Databricks Metric Views & Genie context."""

from __future__ import annotations

import re

import sqlglot
import yaml
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database
from tick.cli import app
from tick.demo import (
    create_demo_database,
    create_files_demo_landing_zone,
    create_manufacturing_demo_database,
)
from tick.exporters import (
    export_semantic_bundle,
    render_cortex_analyst_yaml,
    render_databricks_genie_context_md,
    render_databricks_metric_views_sql,
    render_html_report,
)
from tick.models import (
    ColumnInfo,
    ColumnProfile,
    DatabaseOverview,
    DiscoveryResult,
    GovernanceFinding,
    PiiFinding,
    RelationshipInfo,
    TableInfo,
    TableRecommendations,
    ValueFrequency,
)

runner = CliRunner()

_MV_BLOCK_RE = re.compile(
    r"CREATE OR REPLACE VIEW\s+(?P<fqn>[^\n]+)\nWITH METRICS\nLANGUAGE YAML\nAS \$\$\n(?P<yaml>.*?)\n\$\$;",
    re.S,
)


def _extract_metric_views(sql_text: str) -> list[tuple[str, dict]]:
    views = []
    for m in _MV_BLOCK_RE.finditer(sql_text):
        parsed = yaml.safe_load(m.group("yaml"))
        assert isinstance(parsed, dict)
        views.append((m.group("fqn").strip(), parsed))
    return views


def _validate_cortex_yaml_invariants(doc: dict) -> None:
    """Assert Snowflake Cortex Analyst structural invariants across tables, PKs, relationships & VQRs."""
    assert doc["name"]
    assert doc["description"]
    assert isinstance(doc["tables"], list)

    tables_by_name: dict[str, dict] = {}
    cols_by_table: dict[str, set[str]] = {}
    unique_cols_by_table: dict[str, set[str]] = {}

    for t in doc["tables"]:
        tname = t["name"]
        assert tname not in tables_by_name, f"Duplicate logical table name {tname}"
        tables_by_name[tname] = t
        assert set(t["base_table"].keys()) == {"database", "schema", "table"}

        col_names: set[str] = set()
        u_cols: set[str] = set()
        for sec in ("dimensions", "time_dimensions", "facts"):
            for item in t.get(sec, []):
                assert item["name"] not in col_names, f"Duplicate column {tname}.{item['name']}"
                col_names.add(item["name"])
                assert item["expr"]
                assert item["data_type"]
                if item.get("unique"):
                    u_cols.add(item["name"])

        if "primary_key" in t:
            pk_cols = t["primary_key"]["columns"]
            assert isinstance(pk_cols, list) and pk_cols
            for pk in pk_cols:
                assert pk in col_names, f"PK {pk} not emitted in logical columns of {tname}"
            if len(pk_cols) == 1:
                u_cols.add(pk_cols[0])

        cols_by_table[tname] = col_names
        unique_cols_by_table[tname] = u_cols

    for rel in doc.get("relationships", []):
        lt = rel["left_table"]
        rt = rel["right_table"]
        assert lt in tables_by_name and rt in tables_by_name
        assert lt != rt
        assert rel["join_type"] in {"left_outer", "inner"}
        assert rel["relationship_type"] in {"many_to_one", "one_to_one"}
        for rc in rel["relationship_columns"]:
            assert rc["left_column"] in cols_by_table[lt]
            assert rc["right_column"] in cols_by_table[rt]
            assert rc["right_column"] in unique_cols_by_table[rt], (
                f"Cortex Analyst target column {rt}.{rc['right_column']} must be unique or PK"
            )

    for vqr in doc.get("verified_queries", []):
        assert vqr["question"]
        parsed = [s for s in sqlglot.parse(vqr["sql"], read="snowflake") if s is not None]
        assert len(parsed) == 1, f"VQR SQL failed Snowflake parse: {vqr['sql']}"


def _validate_metric_views_invariants(sql_text: str) -> list[tuple[str, dict]]:
    """Assert Databricks Unity Catalog Metric Views v1.1 syntax, YAML validity & Spark SQL expressions."""
    views = _extract_metric_views(sql_text)
    assert len(views) >= 1
    seen_fqns: set[str] = set()
    for fqn, spec in views:
        assert fqn not in seen_fqns
        seen_fqns.add(fqn)
        assert spec["version"] == 1.1
        assert spec["source"]
        assert spec["measures"], f"Metric view {fqn} must define at least one measure"

        dim_names: set[str] = set()
        for dim in spec.get("dimensions", []):
            assert dim["name"] not in dim_names
            dim_names.add(dim["name"])
            assert sqlglot.parse_one(dim["expr"], read="databricks") is not None

        meas_names: set[str] = set()
        for meas in spec["measures"]:
            assert meas["name"] not in meas_names
            assert meas["name"] not in dim_names
            meas_names.add(meas["name"])
            assert sqlglot.parse_one(meas["expr"], read="databricks") is not None

        for j in spec.get("joins", []):
            assert j["name"] and j["source"] and j["on"]
            assert sqlglot.parse_one(j["on"], read="databricks") is not None

    return views


def test_manufacturing_semantic_layer_cortex_metric_views_and_genie(tmp_path):
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    res = discover_database(
        str(db_path),
        config=DiscoveryConfig(
            target_platform="both",
            skip_empty_tables=True,
            databricks_catalog="mfg_prod",
            databricks_schema="bronze_ohio",
            snowflake_database="MFG_PROD",
            snowflake_schema="BRONZE_OHIO",
        ),
    )

    # 1. Snowflake Cortex Analyst YAML
    cortex_yml_text = render_cortex_analyst_yaml(res)
    cortex_doc = yaml.safe_load(cortex_yml_text)
    _validate_cortex_yaml_invariants(cortex_doc)

    tables_by_name = {t["name"]: t for t in cortex_doc["tables"]}
    # Empty SAP Z-table excluded from LLM context
    assert "empty_sap_ztable" not in tables_by_name
    assert "empty source table" in cortex_doc["custom_instructions"].lower()

    # SAP glossary semantic names + physical synonyms + Silver Dynamic Table targets
    assert "material_master" in tables_by_name
    mara = tables_by_name["material_master"]
    assert mara["base_table"] == {"database": "MFG_PROD", "schema": "SILVER", "table": "SILVER_MATERIAL_MASTER"}
    assert "MARA" in mara.get("synonyms", [])

    # SAP AUFK legacy ERDAT + ERZET stitched into Silver TIMESTAMP_NTZ time_dimension
    assert "production_order_master" in tables_by_name
    aufk = tables_by_name["production_order_master"]
    aufk_tdims = {d["name"]: d for d in aufk.get("time_dimensions", [])}
    assert any(d["expr"] == '"ERDAT_TS"' and d["data_type"] == "TIMESTAMP_NTZ" for d in aufk_tdims.values())
    aufk_all_cols = {
        c["name"]
        for sec in ("dimensions", "time_dimensions", "facts")
        for c in aufk.get(sec, [])
    }
    assert "created_time" not in aufk_all_cols  # ERZET folded into ERDAT_TS

    # Gold exploded BOM logical table included
    assert "bill_of_materials_exploded" in tables_by_name
    bom_gold = tables_by_name["bill_of_materials_exploded"]
    assert bom_gold["base_table"] == {"database": "MFG_PROD", "schema": "GOLD", "table": "GOLD_BILL_OF_MATERIALS_EXPLODED"}
    assert any(m["name"] == "total_extended_qty" for m in bom_gold["metrics"])

    # PII & ITAR excluded from semantic model; commercial cost kept as fact without sample_values
    wo = tables_by_name["work_orders"]
    wo_all_names = {
        c["name"]
        for sec in ("dimensions", "time_dimensions", "facts")
        for c in wo.get(sec, [])
    }
    assert "operator_email" not in wo_all_names
    assert "cad_drawing_path" not in wo_all_names
    # Reserved SQL keyword 'Order #' becomes 'order_col' (safe in Snowflake VQR SQL) pointing to '"ORDER"'
    wo_dims = {d["name"]: d for d in wo.get("dimensions", [])}
    assert "order_col" in wo_dims
    assert wo_dims["order_col"]["expr"] == '"ORDER"'
    # Unit cost preserved as quantitative fact/metric (never sample_values)
    wo_facts = {f["name"]: f for f in wo.get("facts", [])}
    assert "unit_cost" in wo_facts
    assert "sample_values" not in wo_facts["unit_cost"]
    # Safe low-cardinality status gets sample_values
    assert "status" in wo_dims
    assert wo_dims["status"].get("sample_values")

    # Verified queries cover trends, OT telemetry, and exploded BOM
    assert len(cortex_doc.get("verified_queries", [])) >= 3

    # 2. Databricks Unity Catalog Metric Views SQL (v1.1)
    mv_sql = render_databricks_metric_views_sql(res)
    assert "CREATE SCHEMA IF NOT EXISTS mfg_prod.gold;" in mv_sql
    views = _validate_metric_views_invariants(mv_sql)
    view_by_short = {fqn.split(".")[-1]: spec for fqn, spec in views}

    assert "mv_work_orders" in view_by_short
    wo_mv = view_by_short["mv_work_orders"]
    assert wo_mv["source"] == "mfg_prod.silver.silver_work_orders"
    wo_mv_dims = {d["name"]: d for d in wo_mv["dimensions"]}
    # Reserved keyword column `order` backtick-quoted in Spark SQL expression!
    assert wo_mv_dims["order_col"]["expr"] == "source.`order`"
    # PII & ITAR excluded from Metric View dimensions
    assert "operator_email" not in wo_mv_dims
    assert "cad_drawing_path" not in wo_mv_dims
    # Currency format hint attached to unit_cost measure
    wo_mv_meas = {m["name"]: m for m in wo_mv["measures"]}
    assert "avg_unit_cost" in wo_mv_meas
    assert wo_mv_meas["avg_unit_cost"]["format"]["type"] == "currency"

    # Gold BOM Metric View emitted
    assert "mv_bill_of_materials_bom" in view_by_short
    bom_mv = view_by_short["mv_bill_of_materials_bom"]
    assert bom_mv["source"] == "mfg_prod.gold.gold_bill_of_materials_exploded"
    assert any(m["name"] == "total_extended_qty" for m in bom_mv["measures"])

    # OT Telemetry Metric View has hourly bucket + reading_count + max reading
    ot_mvs = [spec for name, spec in view_by_short.items() if "historian" in name or "plc" in name]
    assert ot_mvs
    assert any(
        "DATE_TRUNC('HOUR'" in d["expr"] for spec in ot_mvs for d in spec.get("dimensions", [])
    )

    # 3. Databricks AI/BI Genie Context Guide
    genie_md = render_databricks_genie_context_md(res)
    assert "Databricks AI/BI Genie Space Blueprint" in genie_md
    assert "mfg_prod.silver.silver_work_orders" in genie_md
    assert "mfg_prod.gold.mv_work_orders" in genie_md
    assert "MARA = material_master" in genie_md
    assert "WERKS = plant_code" in genie_md
    assert "gold_bill_of_materials_exploded" in genie_md
    # PII/ITAR sample values must never appear in Safe Filter Value Reference
    filter_section = genie_md.split("## 4. Safe Filter Value Reference")[1].split("## 5.")[0]
    assert "operator_email" not in filter_section
    assert "cad_drawing_path" not in filter_section
    assert "unit_cost" not in filter_section
    # Benchmark SQL queries exist and Q1 uses valid MEASURE(...) from the first anchor metric view
    assert "MEASURE(" in genie_md
    first_mv_name, first_mv_spec = views[0]
    first_mv_short = first_mv_name.split(".")[-1]
    assert first_mv_short in genie_md
    q1_section = genie_md.split("### Q1:")[1].split("```")[1]
    m_match = re.search(r"MEASURE\(`?([a-zA-Z0-9_]+)`?\)", q1_section)
    assert m_match is not None
    assert m_match.group(1) in {m["name"] for m in first_mv_spec["measures"]}


def test_ecommerce_star_schema_joins_and_pii_filtering(tmp_path):
    db_path = create_demo_database(tmp_path / "ecom.duckdb")
    res = discover_database(
        str(db_path),
        config=DiscoveryConfig(
            target_platform="both",
            databricks_catalog="retail",
            snowflake_database="RETAIL",
        ),
    )

    # Cortex Analyst emits explicit FK relationships and joined VQR
    cortex_doc = yaml.safe_load(render_cortex_analyst_yaml(res))
    _validate_cortex_yaml_invariants(cortex_doc)
    rels = cortex_doc.get("relationships", [])
    assert len(rels) >= 2
    rel_pairs = {(r["left_table"], r["right_table"]) for r in rels}
    assert ("orders", "customers") in rel_pairs
    assert any("LEFT JOIN" in v["sql"] for v in cortex_doc.get("verified_queries", []))

    # Customer PII columns excluded from Cortex semantic model
    cust_tbl = next(t for t in cortex_doc["tables"] if t["name"] == "customers")
    cust_cols = {
        c["name"]
        for sec in ("dimensions", "time_dimensions", "facts")
        for c in cust_tbl.get(sec, [])
    }
    assert "email" not in cust_cols
    assert "phone" not in cust_cols
    assert "ssn" not in cust_cols

    # Databricks Metric Views includes star-schema join from orders -> customers + customer dimensions
    mv_sql = render_databricks_metric_views_sql(res)
    views = _validate_metric_views_invariants(mv_sql)
    orders_mv = next(spec for fqn, spec in views if fqn.endswith(".mv_orders"))
    assert any(j["name"] == "customers" and "customer_id" in j["on"] for j in orders_mv.get("joins", []))
    # Joined customer non-PII attributes included; PII never pulled in
    joined_dim_names = {d["name"] for d in orders_mv["dimensions"]}
    assert not any("email" in n or "ssn" in n or "phone" in n for n in joined_dim_names)

    # Genie guide lists verified join paths
    genie_md = render_databricks_genie_context_md(res)
    assert "silver_orders.customer_id = silver_customers.id" in genie_md


def test_file_lake_cli_semantic_dir_and_html_code_tabs(tmp_path):
    lake_dir = create_files_demo_landing_zone(tmp_path / "lake_drop")

    # 1. CLI --semantic-dir with --target both
    sem_both = tmp_path / "sem_both"
    r1 = runner.invoke(
        app,
        ["discover", str(lake_dir), "--semantic-dir", str(sem_both), "--target", "both", "--quiet"],
    )
    assert r1.exit_code == 0
    assert {p.name for p in sem_both.iterdir()} == {
        "cortex_analyst_semantic_model.yml",
        "metric_views.sql",
        "genie_context.md",
    }
    cortex_doc = yaml.safe_load((sem_both / "cortex_analyst_semantic_model.yml").read_text(encoding="utf-8"))
    _validate_cortex_yaml_invariants(cortex_doc)
    _validate_metric_views_invariants((sem_both / "metric_views.sql").read_text(encoding="utf-8"))

    # 2. --target snowflake writes ONLY Cortex Analyst YAML
    sem_sf = tmp_path / "sem_sf"
    r2 = runner.invoke(
        app,
        ["discover", str(lake_dir), "--semantic-dir", str(sem_sf), "--target", "snowflake", "--quiet"],
    )
    assert r2.exit_code == 0
    assert {p.name for p in sem_sf.iterdir()} == {"cortex_analyst_semantic_model.yml"}

    # 3. --target databricks writes ONLY Metric Views SQL & Genie Guide
    sem_dbx = tmp_path / "sem_dbx"
    r3 = runner.invoke(
        app,
        ["discover", str(lake_dir), "--semantic-dir", str(sem_dbx), "--target", "databricks", "--quiet"],
    )
    assert r3.exit_code == 0
    assert {p.name for p in sem_dbx.iterdir()} == {"metric_views.sql", "genie_context.md"}

    # 4. HTML report embeds all 3 semantic layer preview tabs & filenames
    res = discover_database(str(lake_dir))
    html_text = render_html_report(res)
    for expected in (
        "Databricks Metric Views",
        "Databricks Genie Guide",
        "Snowflake Cortex Analyst",
        "metric_views.sql",
        "genie_context.md",
        "cortex_analyst_semantic_model.yml",
    ):
        assert expected in html_text


def test_semantic_edge_cases_empty_db_dollar_injection_pii_pk_and_multi_schema(tmp_path):
    # 1. Empty database (0 base tables) produces valid minimal YAML / SQL / Markdown
    empty_res = DiscoveryResult(
        config=DiscoveryConfig(),
        overview=DatabaseOverview(
            dialect="postgresql",
            driver="psycopg",
            database_name="empty_db",
            sanitized_url="postgresql://empty_db",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
        ),
        tables=[],
    )
    empty_cortex = yaml.safe_load(render_cortex_analyst_yaml(empty_res))
    assert empty_cortex["tables"] == []
    assert "No eligible base tables" in render_databricks_metric_views_sql(empty_res)
    assert "Databricks AI/BI Genie Space Blueprint" in render_databricks_genie_context_md(empty_res)

    # 2. Exotic dollar signs ($$) in comments, PII column as PK/join key, redacted values & multi-schema collision
    t_users = TableInfo(
        schema_name="sales",
        name="orders",
        full_name="sales.orders",
        row_count=50,
        comment="Orders $$ with dollar quotes $$",
        primary_key=["user_email"],
        recommendations=TableRecommendations(entity_role="dimension"),
        columns=[
            ColumnInfo(
                name="user_email",
                raw_type="VARCHAR(255)",
                canonical_type="string",
                is_primary_key=True,
                nullable=False,
                comment="Primary key email $$ do not break",
                profile=ColumnProfile(
                    cardinality_class="low",
                    distinct_count=2,
                    top_values=[
                        ValueFrequency(value="alice@corp.com", count=25, rate=0.5),
                        ValueFrequency(value="bob@corp.com", count=25, rate=0.5),
                    ],
                    pii_findings=[PiiFinding(category="email", confidence="high", score=0.95)],
                ),
            ),
            ColumnInfo(
                name="region",
                raw_type="VARCHAR(32)",
                canonical_type="string",
                profile=ColumnProfile(
                    cardinality_class="low",
                    distinct_count=2,
                    top_values=[
                        ValueFrequency(value="[REDACTED]", count=30, rate=0.6),
                        ValueFrequency(value="NA", count=20, rate=0.4),
                    ],
                ),
            ),
        ],
    )
    t_archive = TableInfo(
        schema_name="archive",
        name="orders",
        full_name="archive.orders",
        row_count=100,
        primary_key=["order"],
        recommendations=TableRecommendations(entity_role="fact"),
        columns=[
            ColumnInfo(name="order", raw_type="INT", canonical_type="integer", is_primary_key=True, nullable=False),
            ColumnInfo(name="user_email", raw_type="VARCHAR(255)", canonical_type="string", is_foreign_key=True),
            ColumnInfo(
                name="total_amount",
                raw_type="DECIMAL(12, 2)",
                canonical_type="decimal",
                profile=ColumnProfile(
                    governance_findings=[
                        GovernanceFinding(
                            category="commercial_sensitive",
                            confidence="high",
                            uc_policy="column_mask",
                            uc_tag_key="sensitivity",
                            uc_tag_value="commercial_cost",
                        )
                    ]
                ),
            ),
        ],
    )
    tricky_res = DiscoveryResult(
        config=DiscoveryConfig(target_platform="both"),
        overview=DatabaseOverview(
            dialect="postgresql",
            driver="psycopg",
            database_name="tricky$$db",
            sanitized_url="postgresql://tricky",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=2,
            table_count=2,
            column_count=5,
        ),
        tables=[t_users, t_archive],
        relationships=[
            RelationshipInfo(
                source_table="archive.orders",
                source_column="user_email",
                target_table="sales.orders",
                target_column="user_email",
                kind="explicit_fk",
                confidence=1.0,
            )
        ],
    )

    cortex_doc = yaml.safe_load(render_cortex_analyst_yaml(tricky_res))
    _validate_cortex_yaml_invariants(cortex_doc)
    # Multi-schema collision resolved cleanly
    assert {t["name"] for t in cortex_doc["tables"]} == {"sales_orders", "archive_orders"}
    # PII PK preserved so relationship stays valid, but NEVER emits sample_values!
    sales_t = next(t for t in cortex_doc["tables"] if t["name"] == "sales_orders")
    email_dim = next(d for d in sales_t["dimensions"] if d["name"] == "user_email")
    assert "sample_values" not in email_dim
    assert "Sensitive identifier" in email_dim["description"]
    # [REDACTED] stripped from sample_values
    region_dim = next(d for d in sales_t["dimensions"] if d["name"] == "region")
    assert region_dim["sample_values"] == ["NA"]
    assert len(cortex_doc["relationships"]) == 1

    # Databricks Metric View does not break on $$ in comments/db name!
    mv_sql = render_databricks_metric_views_sql(tricky_res)
    views = _validate_metric_views_invariants(mv_sql)
    assert len(views) >= 1
    for _, spec in views:
        assert "$$" not in yaml.safe_dump(spec)

    # export_semantic_bundle writes cleanly
    written = export_semantic_bundle(tricky_res, tmp_path / "tricky_sem")
    assert len(written) == 3
