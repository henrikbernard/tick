"""Snowflake SQL type mapping, compatibility linter, and clustering/warehouse advisor."""

from __future__ import annotations

import re
from collections.abc import Sequence

from tick.models import (
    CanonicalType,
    SnowflakeCompatibility,
    SnowflakeCompatibilityIssue,
    TableInfo,
)

# Identifiers outside simple uppercase ASCII require double-quoting in Snowflake SQL
UNQUOTED_SF_IDENT_RE = re.compile(r"^[A-Z_][A-Z0-9_$]*$")
SF_DECIMAL_PREC_RE = re.compile(r"\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")
SF_TZ_AWARE_HINTS = ("TIMESTAMPTZ", "WITH TIME ZONE", "DATETIMEOFFSET", "TIMESTAMP_LTZ", "TIMESTAMP_TZ")

# Snowflake natively supports GEOGRAPHY and GEOMETRY, unlike Spark/Delta
SF_UNSUPPORTED_TYPES = {"SQL_VARIANT", "HIERARCHYID", "IMAGE", "BFILE", "ROWVERSION", "XML"}


def sanitize_snowflake_identifier(name: str) -> str:
    """Convert raw source table/column names into clean uppercase Snowflake identifiers."""
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", name.strip()).strip("_").upper()
    if not cleaned:
        cleaned = "COL"
    if cleaned[0].isdigit():
        cleaned = f"C_{cleaned}"
    return cleaned


def deduplicate_snowflake_columns(col_names: Sequence[str]) -> dict[str, str]:
    """Deterministically map raw column names to unique uppercase Snowflake identifiers."""
    seen: set[str] = set()
    mapping: dict[str, str] = {}
    for raw in col_names:
        base = sanitize_snowflake_identifier(raw)
        candidate = base
        suffix = 2
        while candidate in seen:
            candidate = f"{base}_{suffix}"
            suffix += 1
        seen.add(candidate)
        mapping[raw] = candidate
    return mapping


def deduplicate_snowflake_tables(tables: Sequence[TableInfo]) -> dict[str, str]:
    """Map TableInfo.full_name to unique uppercase Snowflake table identifiers across schemas."""
    distinct_schemas = {t.schema_name for t in tables}
    seen: set[str] = set()
    mapping: dict[str, str] = {}
    for tbl in tables:
        raw_base = tbl.semantic_name or tbl.name
        if len(distinct_schemas) > 1:
            base = sanitize_snowflake_identifier(f"{tbl.schema_name}_{raw_base}")
        else:
            base = sanitize_snowflake_identifier(raw_base)
        candidate = base
        suffix = 2
        while candidate in seen:
            candidate = f"{base}_{suffix}"
            suffix += 1
        seen.add(candidate)
        mapping[tbl.full_name] = candidate
    return mapping


def map_to_snowflake_type(raw_type: str, canonical: CanonicalType) -> str:
    """Map a source SQL data type to the best native Snowflake SQL type."""
    u = raw_type.strip().upper()
    base = re.split(r"[\s(<]", u, maxsplit=1)[0]

    # Snowflake has first-class geospatial types
    if base in {"GEOGRAPHY", "SDO_GEOMETRY"}:
        return "GEOGRAPHY"
    if base == "GEOMETRY":
        return "GEOMETRY"

    if canonical == "integer":
        if base in {"DECIMAL", "NUMERIC", "NUMBER", "DEC", "FIXED"}:
            m = SF_DECIMAL_PREC_RE.search(u)
            if m:
                prec = min(int(m.group(1)), 38)
                return f"NUMBER({prec}, 0)"
        if any(k in base for k in ("BIGINT", "BIGSERIAL", "INT8", "INT64", "HUGEINT", "UBIGINT", "UINT64")):
            return "BIGINT"
        if any(k in base for k in ("SMALLINT", "INT2", "TINYINT")):
            return "SMALLINT"
        return "INTEGER"

    if canonical == "decimal":
        if base in {"MONEY", "SMALLMONEY"}:
            return "NUMBER(19, 4)"
        m = SF_DECIMAL_PREC_RE.search(u)
        if m:
            prec = max(1, int(m.group(1)))
            scale = int(m.group(2)) if m.group(2) else 0
            if prec > 38:
                return f"NUMBER(38, {min(scale, 18)})"
            return f"NUMBER({prec}, {min(scale, prec)})"
        return "NUMBER(18, 4)"

    if canonical == "float":
        return "FLOAT"
    if canonical == "boolean":
        return "BOOLEAN"
    if canonical == "date":
        return "DATE"
    if canonical == "timestamp":
        if any(h in u for h in SF_TZ_AWARE_HINTS):
            return "TIMESTAMP_LTZ"
        return "TIMESTAMP_NTZ"
    if canonical == "time":
        return "TIME"
    if canonical == "json":
        return "VARIANT"
    if canonical == "binary":
        return "BINARY"
    return "VARCHAR"


def _choose_snowflake_clustering_keys(table: TableInfo, col_map: dict[str, str]) -> list[str]:
    """Recommend up to 4 Snowflake clustering keys (plant key -> watermark -> FK/filter keys -> PK)."""
    if table.table_type != "table":
        return []
    if table.telemetry_profile and table.telemetry_profile.recommended_liquid_cluster:
        return [
            col_map.get(c, sanitize_snowflake_identifier(c))
            for c in table.telemetry_profile.recommended_liquid_cluster[:4]
        ]

    keys: list[str] = []

    # 1. Plant / site isolation key first for strong multi-plant micro-partition pruning
    for col in table.columns:
        if col.profile and any(g.category == "plant_isolation_key" for g in col.profile.governance_findings):
            keys.append(col_map.get(col.name, sanitize_snowflake_identifier(col.name)))
            break

    # 2. Primary temporal / watermark column
    if table.recommendations and table.recommendations.watermark_candidates:
        wm = table.recommendations.watermark_candidates[0]
        sname = col_map.get(wm.column, sanitize_snowflake_identifier(wm.column))
        if sname not in keys:
            keys.append(sname)

    # 3. High-selectivity FK / clustering candidates
    if table.recommendations:
        for c in table.recommendations.clustering_candidates:
            sc = col_map.get(c, sanitize_snowflake_identifier(c))
            if sc not in keys:
                keys.append(sc)
            if len(keys) >= 4:
                break

    # 4. Fallback to PK
    if not keys and table.primary_key:
        pk0 = table.primary_key[0]
        keys.append(col_map.get(pk0, sanitize_snowflake_identifier(pk0)))

    return keys[:4]


def _recommend_warehouse_size(table: TableInfo) -> str:
    est_mb = table.ingestion_sizing.estimated_size_mb if table.ingestion_sizing else 0.0
    daily = (table.ingestion_sizing.estimated_daily_rows or 0) if table.ingestion_sizing else 0
    if est_mb >= 50_000 or daily >= 10_000_000:
        return "LARGE"
    if est_mb >= 5_000 or daily >= 1_000_000:
        return "MEDIUM"
    if est_mb >= 250 or daily >= 100_000:
        return "SMALL"
    return "XSMALL"


def lint_snowflake_compatibility(tables: list[TableInfo]) -> None:
    """Lint all tables for Snowflake compatibility and compute Snowflake SQL types, clustering & warehouse sizing."""
    for tbl in tables:
        issues: list[SnowflakeCompatibilityIssue] = []
        requires_quoted = False
        col_map = deduplicate_snowflake_columns([c.name for c in tbl.columns])

        for col in tbl.columns:
            col.snowflake_type = map_to_snowflake_type(col.raw_type, col.canonical_type)
            u = col.raw_type.strip().upper()
            base = re.split(r"[\s(<]", u, maxsplit=1)[0]

            # 1. Identifiers requiring double-quoting in Snowflake (spaces/symbols/punctuation)
            if not UNQUOTED_SF_IDENT_RE.match(col.name.upper()):
                requires_quoted = True
                safe = col_map[col.name]
                issues.append(
                    SnowflakeCompatibilityIssue(
                        severity="medium",
                        category="quoted_identifier",
                        column=col.name,
                        message=f"Column '{col.name}' contains special characters/spaces requiring double-quoting in Snowflake.",
                        remediation=f"Quote as \"{col.name}\" in Bronze and alias to {safe} in Silver Dynamic Table.",
                    )
                )

            # 2. NUMBER precision > 38 (Snowflake limit)
            if col.canonical_type == "decimal" or base in {"DECIMAL", "NUMERIC", "NUMBER", "DEC", "FIXED"}:
                m = SF_DECIMAL_PREC_RE.search(u)
                if m and int(m.group(1)) > 38:
                    issues.append(
                        SnowflakeCompatibilityIssue(
                            severity="high",
                            category="decimal_precision_overflow",
                            column=col.name,
                            message=f"Column '{col.name}' has precision {m.group(1)} > Snowflake NUMBER maximum of 38.",
                            remediation="Cast to NUMBER(38, s) or FLOAT during ingestion.",
                        )
                    )

            # 3. Unsupported vendor types
            if base in SF_UNSUPPORTED_TYPES:
                issues.append(
                    SnowflakeCompatibilityIssue(
                        severity="high",
                        category="unsupported_type",
                        column=col.name,
                        message=f"Source type '{col.raw_type}' has no native Snowflake equivalent.",
                        remediation="Cast to VARCHAR or BINARY in source extract view before Bronze ingestion.",
                    )
                )

            # 4. Timezone-naive timestamps
            if col.canonical_type == "timestamp" and not any(h in u for h in SF_TZ_AWARE_HINTS):
                issues.append(
                    SnowflakeCompatibilityIssue(
                        severity="low",
                        category="timezone_naive_timestamp",
                        column=col.name,
                        message=f"Timestamp '{col.name}' ({col.raw_type}) is timezone-naive — verify plant local time vs UTC.",
                        remediation="Land as TIMESTAMP_NTZ and convert with CONVERT_TIMEZONE() in Silver Dynamic Table.",
                    )
                )

            # 5. VARIANT 16 MB limit advisory
            if col.canonical_type == "json":
                issues.append(
                    SnowflakeCompatibilityIssue(
                        severity="low",
                        category="variant_size_limit",
                        column=col.name,
                        message=f"Semi-structured column '{col.name}' maps to Snowflake VARIANT (16 MB compressed max per value).",
                        remediation="Verify payloads stay under 16 MB or split oversized blobs into staged files.",
                    )
                )

        # 6. Auto Clustering cost advisory for small tables
        est_mb = tbl.ingestion_sizing.estimated_size_mb if tbl.ingestion_sizing else 0.0
        clustering_keys = _choose_snowflake_clustering_keys(tbl, col_map)
        if tbl.table_type == "table" and clustering_keys and est_mb < 1024 and tbl.telemetry_profile is None:
            issues.append(
                SnowflakeCompatibilityIssue(
                    severity="low",
                    category="clustering_advisory",
                    column=clustering_keys[0],
                    message=f"Estimated size ({est_mb:.1f} MB) is < 1 GB; Snowflake Automatic Clustering is only cost-effective on large multi-GB/TB tables.",
                    remediation="Load data ordered by watermark/plant key to leverage free natural micro-partition pruning before enabling CLUSTER BY.",
                )
            )

        has_high = any(i.severity == "high" for i in issues)
        tbl.snowflake_compatibility = SnowflakeCompatibility(
            is_compatible=not has_high,
            requires_quoted_identifiers=requires_quoted,
            clustering_keys=clustering_keys,
            enable_change_tracking=True,
            recommended_warehouse_size=_recommend_warehouse_size(tbl),
            issues=issues,
        )
