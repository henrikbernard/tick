"""Spark SQL / Delta Lake type mapping, compatibility linter, and Liquid Clustering advisor."""

from __future__ import annotations

import re

from collections.abc import Sequence

from tick.models import (
    CanonicalType,
    ColumnInfo,
    DeltaCompatibility,
    DeltaCompatibilityIssue,
    TableInfo,
)

# Delta Lake without column mapping forbids these characters in column names
ILLEGAL_DELTA_COL_RE = re.compile(r"[ ,;{}()\n\t=]")
DECIMAL_PREC_RE = re.compile(r"\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")
UNSUPPORTED_SPARK_TYPES = {"SQL_VARIANT", "HIERARCHYID", "GEOGRAPHY", "GEOMETRY", "XML", "ROWVERSION", "IMAGE", "BFILE"}
TZ_AWARE_HINTS = ("TIMESTAMPTZ", "WITH TIME ZONE", "DATETIMEOFFSET", "TIMESTAMP_LTZ", "TIMESTAMP_TZ")


def sanitize_delta_column_name(name: str) -> str:
    """Convert legacy SQL Server / ERP column names (e.g. 'Part #', 'Qty/Hr') to Delta-safe snake_case."""
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", name.strip()).strip("_").lower()
    if not cleaned:
        cleaned = "col"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned


def deduplicate_column_names(col_names: Sequence[str]) -> dict[str, str]:
    """Deterministically map raw column names to unique Delta-safe snake_case names."""
    seen: set[str] = set()
    mapping: dict[str, str] = {}
    for raw in col_names:
        base = sanitize_delta_column_name(raw)
        candidate = base
        suffix = 2
        while candidate in seen:
            candidate = f"{base}_{suffix}"
            suffix += 1
        seen.add(candidate)
        mapping[raw] = candidate
    return mapping


def deduplicate_table_names(tables: Sequence[TableInfo]) -> dict[str, str]:
    """Map TableInfo.full_name to unique Delta/Python-safe table identifiers across schemas."""
    distinct_schemas = {t.schema_name for t in tables}
    seen: set[str] = set()
    mapping: dict[str, str] = {}
    for tbl in tables:
        raw_base = tbl.semantic_name or tbl.name
        if len(distinct_schemas) > 1:
            base = sanitize_delta_column_name(f"{tbl.schema_name}_{raw_base}")
        else:
            base = sanitize_delta_column_name(raw_base)
        candidate = base
        suffix = 2
        while candidate in seen:
            candidate = f"{base}_{suffix}"
            suffix += 1
        seen.add(candidate)
        mapping[tbl.full_name] = candidate
    return mapping


def map_to_spark_type(raw_type: str, canonical: CanonicalType) -> str:
    """Map a source SQL type to the best Databricks / Spark SQL Delta Lake type."""
    u = raw_type.strip().upper()
    base = re.split(r"[\s(]", u, maxsplit=1)[0]

    if canonical == "integer":
        if base in {"DECIMAL", "NUMERIC", "NUMBER", "DEC", "FIXED"}:
            m = DECIMAL_PREC_RE.search(u)
            if m:
                prec = int(m.group(1))
                if prec > 18:
                    return f"DECIMAL({min(prec, 38)}, 0)"
                if prec > 9:
                    return "BIGINT"
                return "INT"
        if any(k in base for k in ("BIGINT", "BIGSERIAL", "INT8", "INT64", "HUGEINT", "UBIGINT", "UINT64")):
            return "BIGINT"
        if any(k in base for k in ("SMALLINT", "INT2", "TINYINT")):
            return "SMALLINT"
        return "INT"
    if canonical == "decimal":
        if base in {"MONEY", "SMALLMONEY"}:
            return "DECIMAL(19, 4)"
        m = DECIMAL_PREC_RE.search(u)
        if m:
            prec = int(m.group(1))
            scale = int(m.group(2)) if m.group(2) else 0
            if prec > 38:
                return f"DECIMAL(38, {min(scale, 18)})"
            return f"DECIMAL({prec}, {scale})"
        return "DECIMAL(18, 4)"
    if canonical == "float":
        return "DOUBLE"
    if canonical == "boolean":
        return "BOOLEAN"
    if canonical == "date":
        return "DATE"
    if canonical == "timestamp":
        if any(h in u for h in TZ_AWARE_HINTS):
            return "TIMESTAMP"
        return "TIMESTAMP_NTZ"
    if canonical == "time":
        return "STRING"
    if canonical == "json":
        return "VARIANT"
    if canonical == "binary":
        return "BINARY"
    if canonical in {"uuid", "string"}:
        return "STRING"
    return "STRING"


def _choose_liquid_clustering_keys(table: TableInfo, col_map: dict[str, str]) -> list[str]:
    """Recommend up to 4 Databricks Liquid Clustering keys (modern replacement for partitioning + Z-ORDER)."""
    if table.table_type != "table":
        return []
    if table.telemetry_profile and table.telemetry_profile.recommended_liquid_cluster:
        return [
            col_map.get(c, sanitize_delta_column_name(c))
            for c in table.telemetry_profile.recommended_liquid_cluster[:4]
        ]

    keys: list[str] = []

    # 1. Plant / site isolation key first (huge win for multi-plant query pruning!)
    for col in table.columns:
        if col.profile and any(g.category == "plant_isolation_key" for g in col.profile.governance_findings):
            keys.append(col_map.get(col.name, sanitize_delta_column_name(col.name)))
            break

    # 2. Primary temporal / watermark column
    if table.recommendations and table.recommendations.watermark_candidates:
        wm = table.recommendations.watermark_candidates[0]
        sname = col_map.get(wm.column, sanitize_delta_column_name(wm.column))
        if sname not in keys:
            keys.append(sname)

    # 3. High-selectivity clustering / FK keys
    if table.recommendations:
        for c in table.recommendations.clustering_candidates:
            sc = col_map.get(c, sanitize_delta_column_name(c))
            if sc not in keys:
                keys.append(sc)
            if len(keys) >= 4:
                break

    # 4. Fallback to PK if nothing else
    if not keys and table.primary_key:
        pk0 = table.primary_key[0]
        keys.append(col_map.get(pk0, sanitize_delta_column_name(pk0)))

    return keys[:4]


def lint_databricks_compatibility(tables: list[TableInfo]) -> None:
    """Lint all tables for Spark/Delta Lake compatibility and compute Spark SQL types & Liquid Clustering."""
    for tbl in tables:
        issues: list[DeltaCompatibilityIssue] = []
        requires_col_mapping = False
        col_map = deduplicate_column_names([c.name for c in tbl.columns])

        for col in tbl.columns:
            col.spark_type = map_to_spark_type(col.raw_type, col.canonical_type)
            u = col.raw_type.strip().upper()
            base = re.split(r"[\s(]", u, maxsplit=1)[0]

            # 1. Illegal Delta column name characters
            if ILLEGAL_DELTA_COL_RE.search(col.name):
                requires_col_mapping = True
                safe = sanitize_delta_column_name(col.name)
                issues.append(
                    DeltaCompatibilityIssue(
                        severity="high",
                        category="illegal_column_name",
                        column=col.name,
                        message=f"Column '{col.name}' contains Delta-illegal characters (spaces/punctuation).",
                        remediation=f"Rename to '{safe}' in Bronze/Silver or set 'delta.columnMapping.mode' = 'name'.",
                    )
                )

            # 2. Decimal precision > 38 (Spark limit) — check raw decimal/numeric types even if scale=0
            if col.canonical_type == "decimal" or base in {"DECIMAL", "NUMERIC", "NUMBER", "DEC", "FIXED"}:
                m = DECIMAL_PREC_RE.search(u)
                if m and int(m.group(1)) > 38:
                    issues.append(
                        DeltaCompatibilityIssue(
                            severity="high",
                            category="decimal_precision_overflow",
                            column=col.name,
                            message=f"Column '{col.name}' has precision {m.group(1)} > Spark maximum of 38.",
                            remediation="Cast to DECIMAL(38, s) or DOUBLE during ingestion.",
                        )
                    )
                elif base in {"MONEY", "SMALLMONEY"}:
                    issues.append(
                        DeltaCompatibilityIssue(
                            severity="low",
                            category="money_type",
                            column=col.name,
                            message=f"Source type '{col.raw_type}' maps to DECIMAL(19, 4) in Spark/Delta.",
                            remediation="Explicitly cast to DECIMAL(19, 4) in Silver.",
                        )
                    )

            # 3. Unsupported / exotic vendor types
            if base in UNSUPPORTED_SPARK_TYPES:
                issues.append(
                    DeltaCompatibilityIssue(
                        severity="high",
                        category="unsupported_type",
                        column=col.name,
                        message=f"Source type '{col.raw_type}' has no native Spark/Delta equivalent.",
                        remediation="Cast to STRING or BINARY in source extract view before Bronze ingestion.",
                    )
                )

            # 4. Timezone-naive timestamps (common multi-plant manufacturing trap!)
            if col.canonical_type == "timestamp" and not any(h in u for h in TZ_AWARE_HINTS):
                issues.append(
                    DeltaCompatibilityIssue(
                        severity="low",
                        category="timezone_naive_timestamp",
                        column=col.name,
                        message=f"Timestamp '{col.name}' ({col.raw_type}) is timezone-naive — verify plant local shift time vs UTC.",
                        remediation="Land as TIMESTAMP_NTZ in Bronze and normalize to UTC using plant timezone lookup in Silver.",
                    )
                )

        # 5. Advise Liquid Clustering instead of legacy Hive partitioning for small/medium tables
        est_mb = tbl.ingestion_sizing.estimated_size_mb if tbl.ingestion_sizing else 0.0
        if tbl.table_type == "table" and tbl.recommendations and tbl.recommendations.partition_candidates and est_mb < 1024:
            issues.append(
                DeltaCompatibilityIssue(
                    severity="low",
                    category="over_partitioning",
                    column=tbl.recommendations.partition_candidates[0].column,
                    message=f"Estimated size ({est_mb:.1f} MB) is < 1 GB; avoid Hive PARTITIONED BY (creates small files).",
                    remediation="Use Databricks Liquid Clustering (CLUSTER BY) instead of physical partitioning.",
                )
            )

        liquid_keys = _choose_liquid_clustering_keys(tbl, col_map)
        has_high = any(i.severity == "high" for i in issues)

        tbl.delta_compatibility = DeltaCompatibility(
            is_compatible=not has_high,
            requires_column_mapping=requires_col_mapping,
            liquid_clustering_keys=liquid_keys,
            enable_change_data_feed=True,
            enable_deletion_vectors=True,
            issues=issues,
        )
