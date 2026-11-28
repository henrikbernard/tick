"""Deployment-ready project bundles: Databricks Asset Bundles + Lakeview Dashboard & Snowflake CLI + Streamlit."""

from __future__ import annotations

import json
import re
from typing import Any

import yaml

from tick.databricks import deduplicate_column_names, deduplicate_table_names, sanitize_delta_column_name
from tick.exporters.file_ingest_export import has_file_sources
from tick.exporters.gold_sql import _find_quantity_column, get_hierarchy_tables
from tick.models import DiscoveryResult, TableInfo
from tick.snowflake import deduplicate_snowflake_columns, deduplicate_snowflake_tables, sanitize_snowflake_identifier


def _slug(val: str) -> str:
    s = re.sub(r"[^a-z0-9_]+", "_", val.lower()).strip("_") or "tick_project"
    if s[0].isdigit():
        s = f"p_{s}"
    return s


def _bt(val: str) -> str:
    return f"`{val.replace('`', '``')}`"


def _dq(val: str) -> str:
    return f'"{val.replace(chr(34), chr(34) * 2)}"'


def _pick_primary_fact_table(base_tables: list[TableInfo]) -> tuple[TableInfo, str, str] | None:
    """Pick the best fact/order table with a temporal column and numeric measure for dashboard trending."""
    scored: list[tuple[int, TableInfo, str, str]] = []
    for tbl in base_tables:
        if tbl.telemetry_profile is not None or tbl.hierarchy is not None:
            continue
        ts_col = next((c.name for c in tbl.columns if c.canonical_type in {"timestamp", "date"}), None)
        num_col = next(
            (
                c.name
                for c in tbl.columns
                if c.canonical_type in {"decimal", "float", "integer"}
                and not c.is_primary_key
                and not c.is_foreign_key
            ),
            None,
        )
        if not (ts_col and num_col):
            continue
        role_bonus = 3 if (tbl.recommendations and tbl.recommendations.entity_role == "fact") else 1
        scored.append((role_bonus * 1000 + (tbl.row_count or 0), tbl, ts_col, num_col))
    if not scored:
        return None
    scored.sort(key=lambda x: -x[0])
    _, tbl, ts_col, num_col = scored[0]
    return tbl, ts_col, num_col


def render_databricks_bundle_yml(
    result: DiscoveryResult,
    catalog: str | None = None,
    bronze_schema: str | None = None,
    silver_schema: str = "silver",
    gold_schema: str = "gold",
) -> str:
    """Render a deployment-ready Databricks Asset Bundle (DABs) databricks.yml manifest."""
    cat = catalog or result.config.databricks_catalog
    bsch = bronze_schema or result.config.databricks_schema
    slug = _slug(result.overview.database_name)
    has_hier = bool(get_hierarchy_tables(result))

    pipeline_key = f"{slug}_lakeflow_pipeline"
    job_key = f"{slug}_orchestration_job"
    dash_key = f"{slug}_observability_dashboard"

    tasks: list[dict[str, Any]] = [
        {
            "task_key": "apply_unity_catalog_ddl",
            "sql_task": {
                "file": {"path": "./unity_catalog_ddl.sql"},
                "warehouse_id": "${var.warehouse_id}",
            },
        },
        {
            "task_key": "run_lakeflow_silver_pipeline",
            "depends_on": [{"task_key": "apply_unity_catalog_ddl"}],
            "pipeline_task": {
                "pipeline_id": f"${{resources.pipelines.{pipeline_key}.id}}",
            },
        },
    ]
    if has_hier:
        tasks.append(
            {
                "task_key": "build_gold_hierarchies",
                "depends_on": [{"task_key": "run_lakeflow_silver_pipeline"}],
                "sql_task": {
                    "file": {"path": "./gold_hierarchies.sql"},
                    "warehouse_id": "${var.warehouse_id}",
                },
            }
        )
    mv_deps = [{"task_key": "build_gold_hierarchies"}] if has_hier else [{"task_key": "run_lakeflow_silver_pipeline"}]
    tasks.append(
        {
            "task_key": "publish_gold_metric_views",
            "depends_on": mv_deps,
            "sql_task": {
                "file": {"path": "./metric_views.sql"},
                "warehouse_id": "${var.warehouse_id}",
            },
        }
    )

    bundle: dict[str, Any] = {
        "bundle": {
            "name": f"tick_{slug}",
        },
        "variables": {
            "catalog": {
                "description": "Target Unity Catalog name",
                "default": cat,
            },
            "bronze_schema": {
                "description": "Bronze landing schema",
                "default": bsch,
            },
            "silver_schema": {
                "description": "Silver Lakeflow Declarative Pipeline schema",
                "default": silver_schema,
            },
            "gold_schema": {
                "description": "Gold analytics & BOM explosion schema",
                "default": gold_schema,
            },
            "warehouse_id": {
                "description": "Databricks SQL Warehouse ID for DDL, Gold SQL, and AI/BI Dashboard",
                "default": "REPLACE_WITH_SQL_WAREHOUSE_ID",
            },
        },
        "resources": {
            "pipelines": {
                pipeline_key: {
                    "name": f"[${{bundle.target}}] {result.overview.database_name} Lakeflow Silver Pipeline",
                    "catalog": "${var.catalog}",
                    "target": "${var.silver_schema}",
                    "serverless": True,
                    "channel": "CURRENT",
                    "configuration": {
                        "bronze_catalog": "${var.catalog}",
                        "bronze_schema": "${var.bronze_schema}",
                    },
                    "libraries": [
                        {"file": {"path": "./lakeflow_pipeline.py"}},
                    ],
                }
            },
            "jobs": {
                job_key: {
                    "name": f"[${{bundle.target}}] {result.overview.database_name} Bronze-Silver-Gold Orchestration",
                    "tasks": tasks,
                }
            },
            "dashboards": {
                dash_key: {
                    "display_name": f"[${{bundle.target}}] {result.overview.database_name} Operational Dashboard",
                    "file_path": "./lakeview_dashboard.lvdash.json",
                    "warehouse_id": "${var.warehouse_id}",
                }
            },
        },
        "targets": {
            "dev": {
                "mode": "development",
                "default": True,
                "variables": {
                    "catalog": f"{cat}_dev",
                },
            },
            "prod": {
                "mode": "production",
                "variables": {
                    "catalog": cat,
                },
            },
        },
    }

    header = (
        f"# Auto-generated by tick: Databricks Asset Bundle (DABs) for {result.overview.database_name}\n"
        "# Deploy with: databricks bundle validate && databricks bundle deploy -t dev\n\n"
    )
    return header + yaml.safe_dump(bundle, sort_keys=False, width=100)


def render_lakeview_dashboard_json(
    result: DiscoveryResult,
    catalog: str | None = None,
    bronze_schema: str | None = None,
    silver_schema: str = "silver",
    gold_schema: str = "gold",
) -> str:
    """Render a Databricks AI/BI Lakeview dashboard (.lvdash.json) tailored to discovered tables & signals."""
    cat = catalog or result.config.databricks_catalog
    bsch = bronze_schema or result.config.databricks_schema
    base_tables = [t for t in result.tables if t.table_type == "table"]
    tbl_name_map = deduplicate_table_names(base_tables)

    datasets: list[dict[str, Any]] = []
    layout: list[dict[str, Any]] = []
    y_cursor = 0

    # 1. Catalog inventory dataset (always present; includes gold_schema when Gold hierarchies exist)
    # Unity Catalog information_schema.tables stores table_schema in lowercase; Spark SQL escapes via backslash
    def _spark_sq(val: str) -> str:
        return val.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")

    bsch_lit = _spark_sq(bsch.lower())
    ssch_lit = _spark_sq(silver_schema.lower())
    gsch_lit = _spark_sq(gold_schema.lower())
    hier_tables = get_hierarchy_tables(result)
    # Include gold_schema always since metric_views.sql publishes Gold Metric Views even without hierarchies
    inv_schemas = list(dict.fromkeys([bsch_lit, ssch_lit, gsch_lit]))
    inv_in_clause = ", ".join(f"'{s}'" for s in inv_schemas)
    datasets.append(
        {
            "name": "uc_catalog_inventory",
            "displayName": "Unity Catalog Table Inventory",
            "query": (
                f"SELECT table_schema, table_name, table_type, created, last_altered, comment\n"
                f"FROM {_bt(cat)}.information_schema.tables\n"
                f"WHERE LOWER(table_schema) IN ({inv_in_clause})\n"
                f"ORDER BY table_schema, table_name"
            ),
        }
    )
    datasets.append(
        {
            "name": "uc_table_count_kpi",
            "displayName": "Discovered Object Count",
            "query": (
                f"SELECT COUNT(*) AS total_tables\n"
                f"FROM {_bt(cat)}.information_schema.tables\n"
                f"WHERE LOWER(table_schema) IN ({inv_in_clause})"
            ),
        }
    )

    layout.append(
        {
            "widget": {
                "name": "kpi_total_tables",
                "queries": [{"name": "main_query", "query": {"datasetName": "uc_table_count_kpi", "fields": [{"name": "total_tables", "expression": "`total_tables`"}], "disaggregated": True}}],
                "spec": {
                    "version": 2,
                    "widgetType": "counter",
                    "encodings": {"value": {"fieldName": "total_tables", "displayName": f"Tables in {cat}"}},
                    "frame": {"showTitle": True, "title": "Managed Lakehouse Tables"},
                },
            },
            "position": {"x": 0, "y": y_cursor, "width": 2, "height": 3},
        }
    )
    layout.append(
        {
            "widget": {
                "name": "tbl_catalog_inventory",
                "queries": [{"name": "main_query", "query": {"datasetName": "uc_catalog_inventory", "fields": [
                    {"name": "table_schema", "expression": "`table_schema`"},
                    {"name": "table_name", "expression": "`table_name`"},
                    {"name": "table_type", "expression": "`table_type`"},
                    {"name": "last_altered", "expression": "`last_altered`"},
                ], "disaggregated": True}}],
                "spec": {
                    "version": 1,
                    "widgetType": "table",
                    "encodings": {
                        "columns": [
                            {"fieldName": "table_schema", "displayName": "Schema"},
                            {"fieldName": "table_name", "displayName": "Table"},
                            {"fieldName": "table_type", "displayName": "Type"},
                            {"fieldName": "last_altered", "displayName": "Last Altered"},
                        ]
                    },
                    "frame": {"showTitle": True, "title": "Bronze & Silver Table Freshness"},
                },
            },
            "position": {"x": 2, "y": y_cursor, "width": 4, "height": 3},
        }
    )
    y_cursor += 3

    # 2. OT Telemetry Quality Trend (if detected)
    ot_tbl = next((t for t in base_tables if t.telemetry_profile is not None), None)
    if ot_tbl and ot_tbl.telemetry_profile:
        tp = ot_tbl.telemetry_profile
        safe_ot = tbl_name_map[ot_tbl.full_name]
        col_map = deduplicate_column_names([c.name for c in ot_tbl.columns])
        ts_c = col_map.get(tp.timestamp_column, sanitize_delta_column_name(tp.timestamp_column))
        val_raw = tp.value_columns[0] if tp.value_columns else None
        val_c = col_map.get(val_raw, sanitize_delta_column_name(val_raw)) if val_raw else None
        q_c = col_map.get(tp.quality_column, sanitize_delta_column_name(tp.quality_column)) if tp.quality_column else None
        q_obj = next((c for c in ot_tbl.columns if c.name == tp.quality_column), None) if tp.quality_column else None

        bad_expr = (
            f"SUM(CASE WHEN {_bt(q_c)} < 192 THEN 1 ELSE 0 END) AS bad_readings"
            if (q_c and q_obj and q_obj.canonical_type in {"integer", "decimal", "float"})
            else (
                f"SUM(CASE WHEN UPPER(CAST({_bt(q_c)} AS STRING)) IN ('BAD','UNCERTAIN','0','FAULT','ERROR') THEN 1 ELSE 0 END) AS bad_readings"
                if q_c
                else "0 AS bad_readings"
            )
        )
        avg_expr = f", AVG({_bt(val_c)}) AS avg_reading" if val_c else ""
        datasets.append(
            {
                "name": "ot_telemetry_health",
                "displayName": f"OT Telemetry Health ({ot_tbl.name})",
                "query": (
                    f"SELECT DATE_TRUNC('hour', {_bt(ts_c)}) AS reading_hour,\n"
                    f"       COUNT(*) AS total_readings,\n"
                    f"       {bad_expr}{avg_expr}\n"
                    f"FROM {_bt(cat)}.{_bt(bsch)}.{_bt(safe_ot)}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY 1"
                ),
            }
        )
        layout.append(
            {
                "widget": {
                    "name": "chart_ot_telemetry",
                    "queries": [{"name": "main_query", "query": {"datasetName": "ot_telemetry_health", "fields": [
                        {"name": "reading_hour", "expression": "`reading_hour`"},
                        {"name": "total_readings", "expression": "`total_readings`"},
                        {"name": "bad_readings", "expression": "`bad_readings`"},
                    ], "disaggregated": True}}],
                    "spec": {
                        "version": 3,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "reading_hour", "scale": {"type": "temporal"}, "displayName": "Hour"},
                            "y": {"fieldName": "total_readings", "scale": {"type": "quantitative"}, "displayName": "Readings"},
                        },
                        "frame": {"showTitle": True, "title": f"OT Sensor Volume & Quality ({ot_tbl.name})"},
                    },
                },
                "position": {"x": 0, "y": y_cursor, "width": 3, "height": 4},
            }
        )

    # 3. Gold BOM / Hierarchy Rollup (if detected)
    if hier_tables:
        htbl = hier_tables[0]
        safe_h = tbl_name_map[htbl.full_name]
        has_qty = _find_quantity_column(htbl) is not None
        qty_agg = ", SUM(`extended_qty`) AS total_extended_qty" if has_qty else ""
        datasets.append(
            {
                "name": "gold_bom_explosion",
                "displayName": f"Gold Hierarchy Depth Rollup ({htbl.name})",
                "query": (
                    f"SELECT `root_node_id`, MAX(`depth`) AS max_depth, COUNT(*) AS component_links{qty_agg}\n"
                    f"FROM {_bt(cat)}.{_bt(gold_schema)}.{_bt(f'gold_{safe_h}_exploded')}\n"
                    f"GROUP BY `root_node_id`\n"
                    f"ORDER BY component_links DESC\n"
                    f"LIMIT 25"
                ),
            }
        )
        layout.append(
            {
                "widget": {
                    "name": "chart_bom_rollup",
                    "queries": [{"name": "main_query", "query": {"datasetName": "gold_bom_explosion", "fields": [
                        {"name": "root_node_id", "expression": "`root_node_id`"},
                        {"name": "component_links", "expression": "`component_links`"},
                        {"name": "max_depth", "expression": "`max_depth`"},
                    ], "disaggregated": True}}],
                    "spec": {
                        "version": 3,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "root_node_id", "scale": {"type": "categorical"}, "displayName": "Root Assembly / Node"},
                            "y": {"fieldName": "component_links", "scale": {"type": "quantitative"}, "displayName": "Exploded Components"},
                        },
                        "frame": {"showTitle": True, "title": f"Gold Recursive BOM Rollup ({htbl.name})"},
                    },
                },
                "position": {"x": 3 if ot_tbl else 0, "y": y_cursor, "width": 3, "height": 4},
            }
        )
    if ot_tbl or hier_tables:
        y_cursor += 4

    # 4. Primary Fact / Production Throughput Trend
    fact_pick = _pick_primary_fact_table(base_tables)
    if fact_pick:
        ftbl, ts_raw, num_raw = fact_pick
        safe_f = tbl_name_map[ftbl.full_name]
        f_col_map = deduplicate_column_names([c.name for c in ftbl.columns])
        ts_c = f_col_map[ts_raw]
        num_c = f_col_map[num_raw]
        datasets.append(
            {
                "name": "primary_fact_daily_trend",
                "displayName": f"Daily Trend ({ftbl.name})",
                "query": (
                    f"SELECT DATE_TRUNC('day', {_bt(ts_c)}) AS activity_day,\n"
                    f"       COUNT(*) AS row_count,\n"
                    f"       SUM({_bt(num_c)}) AS {_bt(f'total_{num_c}')}\n"
                    f"FROM {_bt(cat)}.{_bt(silver_schema)}.{_bt(f'silver_{safe_f}')}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY 1"
                ),
            }
        )
        layout.append(
            {
                "widget": {
                    "name": "chart_primary_fact_trend",
                    "queries": [{"name": "main_query", "query": {"datasetName": "primary_fact_daily_trend", "fields": [
                        {"name": "activity_day", "expression": "`activity_day`"},
                        {"name": "row_count", "expression": "`row_count`"},
                    ], "disaggregated": True}}],
                    "spec": {
                        "version": 3,
                        "widgetType": "line",
                        "encodings": {
                            "x": {"fieldName": "activity_day", "scale": {"type": "temporal"}, "displayName": "Date"},
                            "y": {"fieldName": "row_count", "scale": {"type": "quantitative"}, "displayName": "Daily Volume"},
                        },
                        "frame": {"showTitle": True, "title": f"Daily Silver Activity ({ftbl.name})"},
                    },
                },
                "position": {"x": 0, "y": y_cursor, "width": 6, "height": 4},
            }
        )

    dashboard = {
        "datasets": datasets,
        "pages": [
            {
                "name": "overview",
                "displayName": f"{result.overview.database_name} Overview",
                "layout": layout,
            }
        ],
    }
    return json.dumps(dashboard, indent=2) + "\n"


def render_snowflake_project_yml(
    result: DiscoveryResult,
    database: str | None = None,
    bronze_schema: str | None = None,
    silver_schema: str = "SILVER",
    gold_schema: str = "GOLD",
    warehouse: str | None = None,
) -> str:
    """Render a Snowflake CLI (`snow`) v2 project manifest (snowflake.yml) for SQL + Streamlit deployment."""
    db = database or result.config.snowflake_database
    bsch = bronze_schema or result.config.snowflake_schema
    wh = warehouse or result.config.snowflake_warehouse
    slug = _slug(result.overview.database_name)
    has_hier = bool(get_hierarchy_tables(result))

    sql_scripts = [
        "snowflake_ddl.sql",
    ]
    if has_file_sources(result):
        sql_scripts.append("snowflake_file_ingest.sql")
    sql_scripts.extend(
        [
            "snowflake_dynamic_tables.sql",
            "snowflake_dmf.sql",
        ]
    )
    if has_hier:
        sql_scripts.append("snowflake_gold_hierarchies.sql")

    # Deploy Streamlit into gold_schema when Gold hierarchies create it; otherwise silver_schema (always created)
    sis_schema = gold_schema if has_hier else silver_schema
    manifest: dict[str, Any] = {
        "definition_version": "2",
        "env": {
            "SNOWFLAKE_DATABASE": db,
            "SNOWFLAKE_BRONZE_SCHEMA": bsch,
            "SNOWFLAKE_SILVER_SCHEMA": silver_schema,
            "SNOWFLAKE_GOLD_SCHEMA": gold_schema,
            "SNOWFLAKE_WAREHOUSE": wh,
        },
        "entities": {
            f"tick_{slug}_dashboard": {
                "type": "streamlit",
                "identifier": {
                    "name": f"TICK_{sanitize_snowflake_identifier(result.overview.database_name)}_APP",
                    "schema": sis_schema,
                    "database": db,
                },
                "title": f"tick {result.overview.database_name} Operational Monitor",
                "query_warehouse": wh,
                "main_file": "streamlit_app.py",
                "stage": f"{db}.{sis_schema}.TICK_STREAMLIT_STAGE",
                "artifacts": ["streamlit_app.py"],
            }
        },
    }
    stage_ref = f"{_dq(db)}.{_dq(silver_schema)}.\"SEMANTIC_STAGE\""
    header = (
        f"# Auto-generated by tick: Snowflake CLI (snow) v2 project manifest for {result.overview.database_name}\n"
        "# Deployment sequence:\n"
        + "".join(f"#   snow sql -f {s}\n" for s in sql_scripts)
        + f"#   snow sql -q 'CREATE STAGE IF NOT EXISTS {stage_ref};'\n"
        + f"#   snow stage copy cortex_analyst_semantic_model.yml @{stage_ref} --overwrite\n"
        + "#   snow streamlit deploy --replace\n\n"
    )
    return header + yaml.safe_dump(manifest, sort_keys=False, width=100)


def render_snowflake_streamlit_app(
    result: DiscoveryResult,
    database: str | None = None,
    bronze_schema: str | None = None,
    silver_schema: str = "SILVER",
    gold_schema: str = "GOLD",
) -> str:
    """Render a deployment-ready Streamlit in Snowflake (SiS) observability & manufacturing starter app."""
    db = database or result.config.snowflake_database
    bsch = bronze_schema or result.config.snowflake_schema
    base_tables = [t for t in result.tables if t.table_type == "table"]
    tbl_name_map = deduplicate_snowflake_tables(base_tables)

    ot_tbl = next((t for t in base_tables if t.telemetry_profile is not None), None)
    hier_tables = get_hierarchy_tables(result)

    ot_fqn = None
    ot_ts = None
    ot_val = None
    ot_qual = None
    ot_qual_is_num = False
    if ot_tbl and ot_tbl.telemetry_profile:
        tp = ot_tbl.telemetry_profile
        col_map = deduplicate_snowflake_columns([c.name for c in ot_tbl.columns])
        ot_fqn = f"{_dq(db)}.{_dq(bsch)}.{_dq(tbl_name_map[ot_tbl.full_name])}"
        ot_ts = col_map.get(tp.timestamp_column, sanitize_snowflake_identifier(tp.timestamp_column))
        if tp.value_columns:
            ot_val = col_map.get(tp.value_columns[0], sanitize_snowflake_identifier(tp.value_columns[0]))
        if tp.quality_column:
            ot_qual = col_map.get(tp.quality_column, sanitize_snowflake_identifier(tp.quality_column))
            qobj = next((c for c in ot_tbl.columns if c.name == tp.quality_column), None)
            ot_qual_is_num = bool(qobj and qobj.canonical_type in {"integer", "decimal", "float"})

    gold_bom_fqn = None
    if hier_tables:
        ht = hier_tables[0]
        gold_bom_fqn = f"{_dq(db)}.{_dq(gold_schema)}.{_dq(f'GOLD_{tbl_name_map[ht.full_name]}_EXPLODED')}"

    safe_doc_db = result.overview.database_name.replace("\\", "\\\\").replace('"""', '\\"\\"\\"').replace("\n", " ")
    lines = [
        f'"""Auto-generated Streamlit in Snowflake (SiS) Observability App for {safe_doc_db}."""',
        "",
        "import streamlit as st",
        "from snowflake.snowpark.context import get_active_session",
        "",
        "st.set_page_config(page_title='tick Snowflake Monitor', layout='wide')",
        "session = get_active_session()",
        "",
        f"DATABASE = {db!r}",
        f"BRONZE_SCHEMA = {bsch!r}",
        f"SILVER_SCHEMA = {silver_schema!r}",
        f"GOLD_SCHEMA = {gold_schema!r}",
        "_DB_DQ = DATABASE.replace('\"', '\"\"')",
        "_DB_SQ = DATABASE.replace('\\\\', '\\\\\\\\').replace(\"'\", \"''\")",
        "_BS_SQ = BRONZE_SCHEMA.replace('\\\\', '\\\\\\\\').replace(\"'\", \"''\")",
        "_SS_SQ = SILVER_SCHEMA.replace('\\\\', '\\\\\\\\').replace(\"'\", \"''\")",
        "_GS_SQ = GOLD_SCHEMA.replace('\\\\', '\\\\\\\\').replace(\"'\", \"''\")",
        "",
        f"st.title({f'Snowflake Pipeline & Data Quality Monitor — {result.overview.database_name}'!r})",
        "st.caption(f'Target: `{DATABASE}` · Bronze: `{BRONZE_SCHEMA}` · Silver: `{SILVER_SCHEMA}` · Gold: `{GOLD_SCHEMA}`')",
        "",
        "def _run_df(sql: str):",
        "    try:",
        "        return session.sql(sql).to_pandas()",
        "    except Exception as exc:",
        "        st.warning(f'Query unavailable ({type(exc).__name__}): {exc}')",
        "        return None",
        "",
        "tab_names = ['Catalog & Storage', 'Dynamic Tables Lag', 'DMF Data Quality']",
    ]
    if ot_fqn:
        lines.append("tab_names.append('OT Telemetry Health')")
    if gold_bom_fqn:
        lines.append("tab_names.append('Gold BOM Explosion')")
    lines.extend(
        [
            "tabs = st.tabs(tab_names)",
            "tab_idx = 0",
            "",
            "with tabs[tab_idx]:",
            "    inv_df = _run_df(f'''",
            "        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT,",
            "               ROUND(BYTES / (1024 * 1024), 2) AS SIZE_MB, LAST_ALTERED",
            '        FROM "{_DB_DQ}".INFORMATION_SCHEMA.TABLES',
            "        WHERE TABLE_SCHEMA IN ('{_BS_SQ}', '{_SS_SQ}', '{_GS_SQ}')",
            "        ORDER BY TABLE_SCHEMA, ROW_COUNT DESC NULLS LAST",
            "    ''')",
            "    if inv_df is not None and not inv_df.empty:",
            "        c1, c2, c3 = st.columns(3)",
            "        c1.metric('Managed Objects', int(len(inv_df)))",
            "        c2.metric('Total Rows', f\"{int(inv_df['ROW_COUNT'].fillna(0).sum()):,}\")",
            "        c3.metric('Storage (MB)', f\"{inv_df['SIZE_MB'].fillna(0).sum():,.2f}\")",
            "        st.dataframe(inv_df, use_container_width=True)",
            "tab_idx += 1",
            "",
            "with tabs[tab_idx]:",
            "    dt_df = _run_df(f'''",
            "        SELECT NAME, SCHEMA_NAME, STATE, REFRESH_ACTION, DATA_TIMESTAMP, REFRESH_START_TIME, REFRESH_END_TIME",
            '        FROM TABLE("{_DB_DQ}".INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(RESULT_LIMIT => 50))',
            "        ORDER BY REFRESH_START_TIME DESC",
            "    ''')",
            "    if dt_df is not None:",
            "        st.dataframe(dt_df, use_container_width=True)",
            "tab_idx += 1",
            "",
            "with tabs[tab_idx]:",
            "    dmf_df = _run_df(f'''",
            "        SELECT MEASUREMENT_TIME, TABLE_SCHEMA, TABLE_NAME, METRIC_NAME, VALUE",
            "        FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS",
            "        WHERE TABLE_DATABASE = '{_DB_SQ}'",
            "        ORDER BY MEASUREMENT_TIME DESC",
            "        LIMIT 100",
            "    ''')",
            "    if dmf_df is not None:",
            "        st.dataframe(dmf_df, use_container_width=True)",
            "tab_idx += 1",
            "",
        ]
    )

    if ot_fqn and ot_ts:
        bad_cond = (
            f"CASE WHEN {_dq(ot_qual)} < 192 THEN 1 ELSE 0 END"
            if (ot_qual and ot_qual_is_num)
            else (
                f"CASE WHEN UPPER(TO_VARCHAR({_dq(ot_qual)})) IN ('BAD','UNCERTAIN','0','FAULT','ERROR') THEN 1 ELSE 0 END"
                if ot_qual
                else "0"
            )
        )
        avg_sel = f", AVG({_dq(ot_val)}) AS AVG_READING" if ot_val else ""
        ot_sql = (
            f"SELECT DATE_TRUNC('HOUR', {_dq(ot_ts)}) AS READING_HOUR,\n"
            f"       COUNT(*) AS TOTAL_READINGS,\n"
            f"       SUM({bad_cond}) AS BAD_READINGS{avg_sel}\n"
            f"FROM {ot_fqn}\n"
            f"GROUP BY 1\n"
            f"ORDER BY 1"
        )
        lines.extend(
            [
                "with tabs[tab_idx]:",
                f"    ot_df = _run_df({ot_sql!r})",
                "    if ot_df is not None and not ot_df.empty:",
                "        st.bar_chart(ot_df.set_index('READING_HOUR')[['TOTAL_READINGS', 'BAD_READINGS']])",
                "        st.dataframe(ot_df, use_container_width=True)",
                "tab_idx += 1",
                "",
            ]
        )

    if gold_bom_fqn:
        bom_sql = (
            f"SELECT ROOT_NODE_ID, MAX(DEPTH) AS MAX_DEPTH, COUNT(*) AS COMPONENT_LINKS\n"
            f"FROM {gold_bom_fqn}\n"
            f"GROUP BY ROOT_NODE_ID\n"
            f"ORDER BY COMPONENT_LINKS DESC\n"
            f"LIMIT 50"
        )
        lines.extend(
            [
                "with tabs[tab_idx]:",
                f"    bom_df = _run_df({bom_sql!r})",
                "    if bom_df is not None and not bom_df.empty:",
                "        st.bar_chart(bom_df.set_index('ROOT_NODE_ID')['COMPONENT_LINKS'])",
                "        st.dataframe(bom_df, use_container_width=True)",
                "tab_idx += 1",
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"
