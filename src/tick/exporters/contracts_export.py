"""Data Contract exporters: Databricks Labs DQX YAML and ODCS v3.0.0 Data Contract YAML."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import yaml

from tick.databricks import (
    deduplicate_column_names,
    deduplicate_table_names,
    map_to_spark_type,
    sanitize_delta_column_name,
)
from tick.models import CanonicalType, DiscoveryResult
from tick.snowflake import map_to_snowflake_type

_ODCS_LOGICAL_TYPES: dict[CanonicalType, str] = {
    "integer": "integer",
    "float": "number",
    "decimal": "number",
    "string": "string",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "time": "string",
    "json": "object",
    "binary": "string",
    "uuid": "string",
    "other": "string",
}

_CLASSIFICATION_PRIORITY: dict[str, int] = {
    "ip_or_export_controlled": 0,
    "restricted_pii": 1,
    "commercial_sensitive": 2,
    "plant_isolation_key": 3,
}


def _try_float(val: str | None) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def render_dqx_yaml(
    result: DiscoveryResult,
    catalog: str | None = None,
    schema: str | None = None,
) -> str:
    """Render Databricks Labs DQX quality check rules keyed by target Unity Catalog table."""
    cat = catalog or result.config.databricks_catalog
    sch = schema or result.config.databricks_schema

    base_tables = [t for t in result.tables if t.table_type == "table"]
    tbl_name_map = deduplicate_table_names(base_tables)

    tables_checks: dict[str, list[dict[str, Any]]] = {}

    for tbl in base_tables:
        safe_tbl = tbl_name_map[tbl.full_name]
        col_map = deduplicate_column_names([c.name for c in tbl.columns])
        fqn = f"{cat}.{sch}.{safe_tbl}"
        checks: list[dict[str, Any]] = []

        # Primary key uniqueness check (single or composite)
        if tbl.primary_key:
            pk_safe = [col_map.get(c, sanitize_delta_column_name(c)) for c in tbl.primary_key]
            checks.append(
                {
                    "name": f"pk_unique_{safe_tbl}",
                    "criticality": "error",
                    "check": {
                        "function": "is_unique",
                        "arguments": {"columns": pk_safe},
                    },
                }
            )

        for col in tbl.columns:
            cname = col_map[col.name]
            prof = col.profile
            is_sensitive = bool(
                prof and (prof.pii_findings or prof.governance_findings)
            )

            # 1. Not-null checks
            if col.is_primary_key or (not col.nullable and (prof is None or prof.null_rate == 0.0)):
                checks.append(
                    {
                        "name": f"{cname}_not_null",
                        "criticality": "error",
                        "check": {
                            "function": "is_not_null",
                            "arguments": {"column": cname},
                        },
                    }
                )
            elif prof and not prof.profile_error and prof.sample_size >= 20 and prof.null_rate == 0.0:
                checks.append(
                    {
                        "name": f"{cname}_expected_not_null",
                        "criticality": "warn",
                        "check": {
                            "function": "is_not_null",
                            "arguments": {"column": cname},
                        },
                    }
                )

            # 2. Single-column unique constraint (non-PK)
            if col.is_unique and not col.is_primary_key:
                checks.append(
                    {
                        "name": f"{cname}_unique",
                        "criticality": "error",
                        "check": {
                            "function": "is_unique",
                            "arguments": {"columns": [cname]},
                        },
                    }
                )

            if prof is None or prof.profile_error or is_sensitive:
                continue

            # 3. Categorical domain allowlist only when top_values covers ALL observed distinct values
            if (
                col.canonical_type == "string"
                and prof.cardinality_class == "low"
                and 2 <= (prof.distinct_count or 0) <= 10
                and prof.null_rate < 0.05
                and len(prof.top_values) >= (prof.distinct_count or 0)
            ):
                allowed = [v.value for v in prof.top_values[: prof.distinct_count]]
                checks.append(
                    {
                        "name": f"{cname}_allowed_values",
                        "criticality": "warn",
                        "check": {
                            "function": "is_in_list",
                            "arguments": {"column": cname, "allowed": allowed},
                        },
                    }
                )

            # 4. Observed numeric range bounds (non-ID quantitative measures)
            if (
                col.canonical_type in {"integer", "float", "decimal"}
                and not col.is_primary_key
                and not col.is_foreign_key
                and prof.sample_size >= 20
                and prof.min_value is not None
                and prof.max_value is not None
            ):
                min_f = _try_float(prof.min_value)
                max_f = _try_float(prof.max_value)
                if min_f is not None and max_f is not None and min_f < max_f:
                    checks.append(
                        {
                            "name": f"{cname}_observed_range",
                            "criticality": "warn",
                            "check": {
                                "function": "is_in_range",
                                "arguments": {
                                    "column": cname,
                                    "min_limit": min_f,
                                    "max_limit": max_f,
                                },
                            },
                        }
                    )

        # 5. OT Telemetry quality code check (numeric OPC Classic >= 192 vs string status codes)
        if tbl.telemetry_profile and tbl.telemetry_profile.quality_column:
            raw_qcol = tbl.telemetry_profile.quality_column
            qcol = col_map.get(raw_qcol, sanitize_delta_column_name(raw_qcol))
            qcol_obj = next((c for c in tbl.columns if c.name == raw_qcol), None)
            if qcol_obj and qcol_obj.canonical_type in {"integer", "decimal", "float"}:
                checks.append(
                    {
                        "name": f"{qcol}_good_sensor_quality",
                        "criticality": "error",
                        "check": {
                            "function": "is_not_less_than",
                            "arguments": {"column": qcol, "limit": 192},
                        },
                    }
                )
            else:
                checks.append(
                    {
                        "name": f"{qcol}_good_sensor_quality",
                        "criticality": "error",
                        "check": {
                            "function": "is_not_in_list",
                            "arguments": {
                                "column": qcol,
                                "forbidden": ["BAD", "UNCERTAIN", "FAULT", "ERROR", "0"],
                            },
                        },
                    }
                )

        if checks:
            # Ensure unique check names within each table
            seen_names: set[str] = set()
            for chk in checks:
                base_name = chk["name"]
                cand = base_name
                suffix = 2
                while cand in seen_names:
                    cand = f"{base_name}_{suffix}"
                    suffix += 1
                seen_names.add(cand)
                chk["name"] = cand
            tables_checks[fqn] = checks

    payload = {
        "dqx_version": "0.4",
        "generated_by": "tick",
        "source_database": result.overview.database_name,
        "source_dialect": result.overview.dialect,
        "target_catalog": cat,
        "target_schema": sch,
        "tables": tables_checks,
    }
    return yaml.safe_dump(payload, sort_keys=False, width=100)


def render_odcs_yaml(
    result: DiscoveryResult,
    catalog: str | None = None,
    schema: str | None = None,
) -> str:
    """Render an Open Data Contract Standard (ODCS v3.0.0) YAML contract from discovered metadata."""
    is_sf_only = result.config.target_platform == "snowflake"
    cat = catalog or (result.config.snowflake_database if is_sf_only else result.config.databricks_catalog)
    sch = schema or (result.config.snowflake_schema if is_sf_only else result.config.databricks_schema)
    ov = result.overview

    base_tables = [t for t in result.tables if t.table_type == "table"]
    tbl_name_map = deduplicate_table_names(base_tables)

    schema_objects: list[dict[str, Any]] = []
    for tbl in base_tables:
        safe_tbl = tbl_name_map[tbl.full_name]
        col_map = deduplicate_column_names([c.name for c in tbl.columns])
        pk_positions = {col_name: idx + 1 for idx, col_name in enumerate(tbl.primary_key)}

        properties: list[dict[str, Any]] = []
        table_quality: list[dict[str, Any]] = []

        for col in tbl.columns:
            cname = col_map[col.name]
            if is_sf_only:
                phys_type = col.snowflake_type or map_to_snowflake_type(col.raw_type, col.canonical_type)
            else:
                phys_type = col.spark_type or map_to_spark_type(col.raw_type, col.canonical_type)
            prof = col.profile

            prop: dict[str, Any] = {
                "name": cname,
                "physicalName": col.name,
                "logicalType": _ODCS_LOGICAL_TYPES.get(col.canonical_type, "string"),
                "physicalType": phys_type,
                "required": not col.nullable or col.is_primary_key,
                "unique": col.is_unique or (col.is_primary_key and len(tbl.primary_key) == 1),
            }
            if col.semantic_name:
                prop["businessName"] = col.semantic_name
            if col.business_description or col.comment:
                prop["description"] = col.business_description or col.comment
            if col.name in pk_positions:
                prop["primaryKey"] = True
                prop["primaryKeyPosition"] = pk_positions[col.name]

            tags: list[str] = []
            classifications: list[str] = []
            if prof:
                for pf in prof.pii_findings:
                    tags.append(f"pii:{pf.category}")
                    classifications.append("restricted_pii")
                for gf in prof.governance_findings:
                    tags.append(f"{gf.uc_tag_key}:{gf.uc_tag_value}")
                    classifications.append(gf.category)
                if prof.legacy_date:
                    tags.append(f"legacy_date:{prof.legacy_date.encoding}")

            if classifications:
                prop["classification"] = min(
                    set(classifications),
                    key=lambda c: (_CLASSIFICATION_PRIORITY.get(c, 99), c),
                )
            if tags:
                prop["tags"] = sorted(set(tags))

            properties.append(prop)

        if tbl.primary_key:
            table_quality.append(
                {
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "dimension": "uniqueness",
                    "description": f"Primary key ({', '.join(tbl.primary_key)}) must have zero duplicates.",
                }
            )
        if tbl.row_count and tbl.row_count > 0:
            table_quality.append(
                {
                    "type": "library",
                    "metric": "rowCount",
                    "mustBeGreaterThan": 0,
                    "dimension": "completeness",
                    "description": "Table must contain at least one row after ingestion.",
                }
            )

        obj_entry: dict[str, Any] = {
            "name": safe_tbl,
            "physicalName": tbl.full_name,
            "physicalType": "table",
            "properties": properties,
        }
        if tbl.semantic_name:
            obj_entry["businessName"] = tbl.semantic_name
        if tbl.business_description or tbl.comment:
            obj_entry["description"] = tbl.business_description or tbl.comment
        custom_props: list[dict[str, Any]] = []
        if tbl.delta_compatibility and tbl.delta_compatibility.liquid_clustering_keys:
            custom_props.append(
                {
                    "property": "databricks.liquidClusteringKeys",
                    "value": tbl.delta_compatibility.liquid_clustering_keys,
                }
            )
        if tbl.snowflake_compatibility and tbl.snowflake_compatibility.clustering_keys:
            custom_props.append(
                {
                    "property": "snowflake.clusteringKeys",
                    "value": tbl.snowflake_compatibility.clustering_keys,
                }
            )
        if custom_props:
            obj_entry["customProperties"] = custom_props
        if table_quality:
            obj_entry["quality"] = table_quality

        schema_objects.append(obj_entry)

    servers: list[dict[str, Any]] = [
        {
            "server": "source",
            "type": ov.dialect,
            "description": f"Discovered source database ({ov.sanitized_url})",
        }
    ]
    if result.config.target_platform in {"databricks", "both"}:
        servers.append(
            {
                "server": "databricks_target",
                "type": "databricks",
                "catalog": cat,
                "schema": sch,
                "description": "Target Databricks Unity Catalog Lakehouse schema",
            }
        )
    if result.config.target_platform in {"snowflake", "both"}:
        servers.append(
            {
                "server": "snowflake_target",
                "type": "snowflake",
                "database": result.config.snowflake_database,
                "schema": result.config.snowflake_schema,
                "warehouse": result.config.snowflake_warehouse,
                "description": "Target Snowflake Data Cloud schema",
            }
        )

    contract: dict[str, Any] = {
        "apiVersion": "v3.0.0",
        "kind": "DataContract",
        "id": f"tick-{sanitize_delta_column_name(ov.database_name)}",
        "version": "1.0.0",
        "status": "draft",
        "name": f"{ov.database_name} Data Contract",
        "domain": cat,
        "description": {
            "purpose": f"Auto-discovered data contract for {ov.database_name} ({ov.dialect}) targeting {result.config.target_platform}.",
        },
        "servers": servers,
        "schema": schema_objects,
        "slaProperties": [
            {
                "property": "freshness",
                "value": result.config.stale_after_days,
                "unit": "d",
                "description": "Maximum allowed data staleness before alerting.",
            }
        ],
    }
    return yaml.safe_dump(contract, sort_keys=False, width=100)


def export_contracts_bundle(
    result: DiscoveryResult,
    output_dir: str | Path,
    catalog: str | None = None,
    schema: str | None = None,
) -> list[Path]:
    """Write Databricks DQX YAML and ODCS v3.0.0 Data Contract YAML to output_dir."""
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    if result.config.target_platform in {"databricks", "both"}:
        dqx_path = root / "dqx_checks.yml"
        dqx_path.write_text(render_dqx_yaml(result, catalog=catalog, schema=schema), encoding="utf-8")
        written.append(dqx_path)

    odcs_path = root / "data_contract.odcs.yml"
    odcs_path.write_text(render_odcs_yaml(result, catalog=catalog, schema=schema), encoding="utf-8")
    written.append(odcs_path)

    return written
