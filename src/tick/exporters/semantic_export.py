"""AI / Semantic Layer exporters: Snowflake Cortex Analyst YAML, Databricks Metric Views & Genie context."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from tick.databricks import (
    deduplicate_column_names,
    deduplicate_table_names,
    sanitize_delta_column_name,
)
from tick.exporters.gold_sql import (
    _find_plant_column,
    _find_quantity_column,
    get_hierarchy_tables,
)
from tick.models import ColumnInfo, DiscoveryResult, RelationshipInfo, TableInfo
from tick.snowflake import (
    deduplicate_snowflake_columns,
    deduplicate_snowflake_tables,
    map_to_snowflake_type,
    sanitize_snowflake_identifier,
)

# Numeric columns that are identifiers/codes/buckets rather than quantitative measures
_NON_MEASURE_NUM_RE = re.compile(
    r"(?:^|_)(?:id|ids|key|code|cd|num|number|no|nbr|zip|postal|postcode|year|yr|month|mo|day|week|wk|"
    r"quarter|qtr|port|flag|seq|sequence|rank|version|ver|step|line_no|line_num|posnr|vornr|mandt|"
    r"werks|bukrs|stlnr|aufnr|matnr|kunnr|lifnr|vbeln|ebeln|equnr|prueflos|status|state|type|"
    r"category|tier|priority|shift|channel|index|idx|ordinal|sort_order|quality_code|quality)$"
    r"|^(?:is|has|can|should)_",
    re.I,
)

# Strong semantic measure hints (override weak suffix heuristics if needed)
_MEASURE_HINT_RE = re.compile(
    r"(?:^|_)(?:qty|quantity|amount|amt|total|price|cost|rev|revenue|sales|profit|margin|discount|"
    r"tax|fee|spend|budget|balance|net|gross|subtotal|weight|mass|volume|vol|temp|temperature|"
    r"pressure|bar|psi|speed|rpm|vibration|current|amp|voltage|volt|power|watt|kw|kwh|energy|"
    r"duration|hours|hrs|mins|minutes|secs|seconds|latency|ms|count|cnt|score|pct|percent|ratio|"
    r"rate|yield|scrap|defect|reject|oee|downtime|uptime|throughput|cycle_time|lead_time|"
    r"reading|value|val|units|stock|inventory|on_hand|menge|netwr|stprs|verpr|size|bytes|mb|gb)(?:$|_)",
    re.I,
)

# Measures whose natural default aggregation is AVG (non-additive rates, unit prices, sensor readings)
_AVG_MEASURE_RE = re.compile(
    r"(?:^|_)(?:avg|mean|rate|ratio|pct|percent|score|price|unit_cost|unit_price|stprs|verpr|"
    r"temp|temperature|pressure|bar|psi|speed|rpm|vibration|current|amp|voltage|volt|humidity|"
    r"latency|cycle_time|lead_time|oee|margin|yield|reading|value|val)(?:$|_)",
    re.I,
)

# Currency-like measures (used for Databricks Metric View format hints)
_CURRENCY_MEASURE_RE = re.compile(
    r"(?:^|_)(?:price|cost|rev|revenue|sales|profit|amount|amt|total_amount|net_value|netwr|"
    r"stprs|verpr|spend|budget|tax|fee|discount|balance|subtotal|gross|margin)(?:$|_)",
    re.I,
)

_PERCENT_MEASURE_RE = re.compile(
    r"(?:^|_)(?:pct|percent|percentage|rate|ratio|oee|yield)(?:$|_)",
    re.I,
)

_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Spark SQL reserved/contextual words that should be backtick-quoted in metric view expressions
_SPARK_RESERVED_WORDS = {
    "all", "alter", "and", "anti", "any", "array", "as", "asc", "at", "between", "by", "case",
    "cast", "catalog", "check", "column", "comment", "constraint", "create", "cross", "cube",
    "current", "current_date", "current_time", "current_timestamp", "current_user", "database",
    "date", "day", "default", "delete", "desc", "describe", "distinct", "drop", "else", "end",
    "escape", "except", "exists", "explain", "false", "fetch", "filter", "first", "following",
    "for", "foreign", "from", "full", "function", "grant", "group", "grouping", "having", "hour",
    "if", "in", "index", "inner", "insert", "intersect", "interval", "into", "is", "join", "key",
    "last", "lateral", "leading", "left", "level", "like", "limit", "local", "map", "minute",
    "month", "natural", "not", "null", "nulls", "of", "offset", "on", "only", "or", "order",
    "outer", "over", "partition", "percent", "preceding", "precision", "primary", "range",
    "references", "regexp", "rename", "revoke", "right", "rlike", "rollup", "row", "rows",
    "schema", "second", "select", "semi", "set", "show", "some", "source", "start", "struct",
    "table", "tablesample", "then", "time", "timestamp", "to", "trailing", "true", "truncate",
    "union", "unique", "update", "user", "using", "value", "values", "view", "when", "where",
    "window", "with", "year",
}


# Hard SQL parser-breaking reserved keywords to avoid as bare logical semantic identifiers
_HARD_SQL_RESERVED = {
    "all", "alter", "and", "any", "as", "between", "by", "case", "cast", "check", "column",
    "connect", "constraint", "create", "cross", "current_date", "current_time",
    "current_timestamp", "current_user", "default", "delete", "distinct", "drop", "else", "end",
    "except", "exists", "false", "fetch", "following", "for", "foreign", "from", "full", "grant",
    "group", "having", "ilike", "in", "inner", "insert", "intersect", "into", "is", "join",
    "lateral", "left", "like", "limit", "localtime", "localtimestamp", "merge", "natural", "not",
    "null", "offset", "on", "or", "order", "outer", "over", "partition", "preceding", "primary",
    "qualify", "range", "regexp", "revoke", "right", "rlike", "row", "rows", "sample", "select",
    "set", "some", "start", "table", "tablesample", "then", "to", "true", "try_cast", "union",
    "unique", "update", "user", "using", "values", "view", "when", "where", "window", "with",
}


def _slug(val: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", val.strip().lower()).strip("_") or "item"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned


def _semantic_slug(val: str, suffix: str = "col") -> str:
    """Clean lowercase identifier that also avoids hard SQL parser-breaking reserved words."""
    s = _slug(val)
    if s in _HARD_SQL_RESERVED:
        return f"{s}_{suffix}"
    return s


def _human_label(name: str) -> str:
    cleaned = re.sub(r"[_\-.]+", " ", name.strip()).strip()
    return " ".join(w.capitalize() for w in cleaned.split()) or name


def _clean_text(val: str) -> str:
    """Normalize whitespace and strip any literal '$$' that could break dollar-quoted SQL."""
    text = re.sub(r"\s+", " ", val.replace("$$", "$ $")).strip()
    return text


def _dq(val: str) -> str:
    return f'"{val.replace(chr(34), chr(34) * 2)}"'


def _bt(val: str) -> str:
    return f"`{val.replace('`', '``')}`"


def _spark_ident(val: str) -> str:
    if _SIMPLE_IDENT_RE.match(val) and val.lower() not in _SPARK_RESERVED_WORDS:
        return val
    return _bt(val)


def _spark_table_ref(catalog: str, schema: str, table: str) -> str:
    return ".".join(_spark_ident(p) for p in (catalog, schema, table))


def _escape_md(val: str | None) -> str:
    if not val:
        return "—"
    return val.replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def _is_direct_pii(col: ColumnInfo) -> bool:
    return bool(col.profile and col.profile.pii_findings)


def _is_itar_or_ip(col: ColumnInfo) -> bool:
    return bool(
        col.profile
        and any(g.category == "ip_or_export_controlled" for g in col.profile.governance_findings)
    )


def _is_sensitive_for_samples(col: ColumnInfo) -> bool:
    return bool(col.profile and (col.profile.pii_findings or col.profile.governance_findings))


def _safe_sample_values(col: ColumnInfo, limit: int = 10) -> tuple[list[str], bool]:
    """Return (sample_values, is_enum) only for non-sensitive, low-cardinality string/boolean columns."""
    if _is_sensitive_for_samples(col):
        return [], False
    prof = col.profile
    if prof is None or prof.profile_error:
        return [], False
    if col.canonical_type not in {"string", "boolean"}:
        return [], False
    if prof.cardinality_class not in {"low", "binary", "constant"} and (
        prof.distinct_count is None or prof.distinct_count > 20
    ):
        return [], False

    vals: list[str] = []
    seen: set[str] = set()
    for vf in prof.top_values[:limit]:
        v = str(vf.value).strip()
        if not v or "[REDACTED]" in v or len(v) > 80:
            continue
        if v not in seen:
            seen.add(v)
            vals.append(_clean_text(v))

    is_enum = bool(
        vals
        and prof.distinct_count is not None
        and 1 <= prof.distinct_count <= 15
        and len(vals) >= prof.distinct_count
    )
    return vals, is_enum


def _is_numeric_measure_column(col: ColumnInfo, tbl: TableInfo) -> bool:
    """Determine if a numeric column is a quantitative fact/measure rather than a key, code, or date."""
    if col.canonical_type not in {"decimal", "float", "integer"}:
        return False
    if col.is_primary_key or col.is_foreign_key:
        return False
    if col.profile and col.profile.legacy_date is not None:
        return False
    if tbl.telemetry_profile and tbl.telemetry_profile.quality_column == col.name:
        return False
    if tbl.hierarchy and col.name in {tbl.hierarchy.parent_column, tbl.hierarchy.child_column}:
        return False
    if col.profile and col.profile.cardinality_class == "binary" and col.canonical_type == "integer":
        return False

    name_to_check = col.semantic_name or col.name
    if _MEASURE_HINT_RE.search(name_to_check) or _MEASURE_HINT_RE.search(col.name):
        return True
    if _NON_MEASURE_NUM_RE.search(name_to_check) or _NON_MEASURE_NUM_RE.search(col.name):
        return False
    # Decimals & floats not matching non-measure ID/code patterns are almost always quantitative measures
    if col.canonical_type in {"decimal", "float"}:
        return True
    # Integers: require non-key, non-unique, non-monotonic ID look
    if col.is_unique:
        return False
    if tbl.recommendations and any(
        w.column == col.name and w.kind == "monotonic_id"
        for w in tbl.recommendations.watermark_candidates
    ):
        return False
    return True


def _select_active_base_tables(result: DiscoveryResult) -> tuple[list[TableInfo], list[TableInfo]]:
    """Return (active_tables, omitted_empty_tables). Keeps all base tables if every table is empty."""
    base_tables = [t for t in result.tables if t.table_type == "table" and t.columns]
    non_empty = [t for t in base_tables if t.row_count != 0]
    if non_empty:
        omitted = [t for t in base_tables if t.row_count == 0]
        return non_empty, omitted
    return base_tables, []


def _deduplicate_logical_table_names(tables: list[TableInfo]) -> dict[str, str]:
    """Map TableInfo.full_name -> unique lowercase logical name (preferring semantic_name)."""
    distinct_schemas = {t.schema_name for t in tables}
    seen: set[str] = set()
    mapping: dict[str, str] = {}
    for tbl in tables:
        raw_base = tbl.semantic_name or tbl.name
        if len(distinct_schemas) > 1 and not tbl.semantic_name:
            base = _semantic_slug(f"{tbl.schema_name}_{raw_base}", suffix="tbl")
        else:
            base = _semantic_slug(raw_base, suffix="tbl")
        cand = base
        sfx = 2
        while cand in seen:
            cand = f"{base}_{sfx}"
            sfx += 1
        seen.add(cand)
        mapping[tbl.full_name] = cand
    return mapping


def _compute_snowflake_silver_legacy_cols(
    tbl: TableInfo,
    sf_col_map: dict[str, str],
) -> dict[str, tuple[str, str]]:
    """Mirror snowflake_export.py legacy date column derivation: raw_col -> (sf_col_name, sf_type)."""
    emitted_cols: set[str] = set(sf_col_map.values())
    derived: dict[str, tuple[str, str]] = {}
    for col in tbl.columns:
        if col.profile and col.profile.legacy_date:
            ld = col.profile.legacy_date
            cname = sf_col_map[col.name]
            if ld.encoding == "hhmmss" and ld.paired_column:
                continue
            target_col = f"{cname}_TS" if ld.paired_column else f"{cname}_PARSED"
            if target_col in emitted_cols:
                sfx = 2
                while f"{target_col}_{sfx}" in emitted_cols:
                    sfx += 1
                target_col = f"{target_col}_{sfx}"
            emitted_cols.add(target_col)
            sf_type = "TIMESTAMP_NTZ" if ld.paired_column else "DATE"
            derived[col.name] = (target_col, sf_type)
    return derived


def _compute_databricks_silver_legacy_cols(
    tbl: TableInfo,
    dbx_col_map: dict[str, str],
) -> dict[str, tuple[str, str]]:
    """Mirror databricks_export.py legacy date column derivation: raw_col -> (dbx_col_name, spark_type)."""
    seen_stitched: set[str] = set(dbx_col_map.values())
    derived: dict[str, tuple[str, str]] = {}
    for col in tbl.columns:
        if col.profile and col.profile.legacy_date:
            ld = col.profile.legacy_date
            cname = dbx_col_map[col.name]
            if ld.encoding == "hhmmss" and ld.paired_column:
                continue
            base_target = f"{cname}_ts" if ld.paired_column else f"{cname}_parsed"
            target_col = base_target
            sfx = 2
            while target_col in seen_stitched:
                target_col = f"{base_target}_{sfx}"
                sfx += 1
            seen_stitched.add(target_col)
            spark_type = "TIMESTAMP" if ld.paired_column else "DATE"
            derived[col.name] = (target_col, spark_type)
    return derived


def _collect_join_key_columns(result: DiscoveryResult, min_confidence: float = 0.75) -> set[tuple[str, str]]:
    """Return {(table_full_name, raw_column_name)} participating in verified relationships."""
    keys: set[tuple[str, str]] = set()
    for rel in result.relationships:
        if rel.kind == "explicit_fk" or rel.confidence >= min_confidence:
            keys.add((rel.source_table, rel.source_column))
            keys.add((rel.target_table, rel.target_column))
    return keys


def _synonyms_for_name(logical_name: str, raw_name: str, semantic_name: str | None = None) -> list[str]:
    syns: list[str] = []
    seen = {logical_name.lower()}
    candidates = []
    if semantic_name:
        candidates.append(raw_name)
        candidates.append(_human_label(semantic_name).lower())
    else:
        readable = _human_label(raw_name).lower()
        if readable != logical_name.lower():
            candidates.append(readable)
        if raw_name.lower() != logical_name.lower():
            candidates.append(raw_name)
    for c in candidates:
        c_clean = _clean_text(c)
        if c_clean and c_clean.lower() not in seen:
            seen.add(c_clean.lower())
            syns.append(c_clean)
    return syns[:4]


def _pick_metric_aggregation(col: ColumnInfo) -> str:
    """Return 'avg' for non-additive rates/prices/telemetry readings, else 'sum'."""
    name_to_check = col.semantic_name or col.name
    if _AVG_MEASURE_RE.search(name_to_check) or _AVG_MEASURE_RE.search(col.name):
        return "avg"
    return "sum"


def _filtered_verified_relationships(
    result: DiscoveryResult,
    active_by_full: dict[str, TableInfo],
    min_confidence: float = 0.75,
) -> list[RelationshipInfo]:
    """Return high-confidence, non-self relationships between active tables, deduplicated."""
    out: list[RelationshipInfo] = []
    seen: set[tuple[str, str, str, str]] = set()
    # Explicit FKs first, then highest confidence
    ordered = sorted(
        result.relationships,
        key=lambda r: (0 if r.kind == "explicit_fk" else 1, -r.confidence, r.source_table, r.target_table),
    )
    for rel in ordered:
        if rel.kind != "explicit_fk" and rel.confidence < min_confidence:
            continue
        if rel.source_table == rel.target_table:
            continue
        if rel.source_table not in active_by_full or rel.target_table not in active_by_full:
            continue
        key = (rel.source_table, rel.source_column, rel.target_table, rel.target_column)
        if key in seen:
            continue
        seen.add(key)
        out.append(rel)
    return out


# =============================================================================
# 1. Snowflake Cortex Analyst Semantic Model YAML
# =============================================================================


def render_cortex_analyst_yaml(
    result: DiscoveryResult,
    database: str | None = None,
    silver_schema: str = "SILVER",
    gold_schema: str = "GOLD",
) -> str:
    """Render a Snowflake Cortex Analyst semantic model YAML grounded in discovered tables, joins & glossary."""
    db = database or result.config.snowflake_database
    all_base_tables = [t for t in result.tables if t.table_type == "table"]
    sf_tbl_map = deduplicate_snowflake_tables(all_base_tables)
    active_tables, omitted_empty = _select_active_base_tables(result)
    active_by_full = {t.full_name: t for t in active_tables}
    logical_tbl_map = _deduplicate_logical_table_names(active_tables)
    join_keys = _collect_join_key_columns(result)

    # Track emitted logical columns & unique keys per table for relationship & VQR grounding
    # table_full_name -> raw_col -> logical_col_name
    raw_to_logical_col: dict[str, dict[str, str]] = {}
    # table_full_name -> set of logical_col_names that are PK or unique (valid Cortex many_to_one target)
    unique_logical_cols: dict[str, set[str]] = {}
    # Metadata for verified query synthesis
    table_meta: dict[str, dict[str, Any]] = {}
    excluded_pii_notes: list[str] = []
    plant_tables: list[tuple[str, str]] = []  # (logical_table, logical_plant_col)

    semantic_tables: list[dict[str, Any]] = []

    for tbl in active_tables:
        logical_tname = logical_tbl_map[tbl.full_name]
        safe_sf_tbl = sf_tbl_map[tbl.full_name]
        sf_col_map = deduplicate_snowflake_columns([c.name for c in tbl.columns])
        legacy_sf_map = _compute_snowflake_silver_legacy_cols(tbl, sf_col_map)

        dimensions: list[dict[str, Any]] = []
        time_dimensions: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []
        metrics: list[dict[str, Any]] = []

        seen_col_names: set[str] = set()
        col_raw_to_logical: dict[str, str] = {}
        unique_cols_here: set[str] = set()

        def _alloc_col_name(preferred: str) -> str:
            base = _semantic_slug(preferred, suffix="col")
            cand = base
            sfx = 2
            while cand in seen_col_names:
                cand = f"{base}_{sfx}"
                sfx += 1
            seen_col_names.add(cand)
            return cand

        # Track paired HHMMSS columns already stitched into companion date _TS
        stitched_hhmmss = {
            c.profile.legacy_date.paired_column
            for c in tbl.columns
            if c.profile and c.profile.legacy_date and c.profile.legacy_date.paired_column
        }

        for col in tbl.columns:
            # Skip binary blobs from AI semantic layer
            if col.canonical_type == "binary":
                continue

            is_join_or_pk = col.is_primary_key or (tbl.full_name, col.name) in join_keys

            # Exclude direct PII & ITAR/IP columns unless needed as PK/join key
            if (_is_direct_pii(col) or _is_itar_or_ip(col)) and not is_join_or_pk:
                cats = [f.category for f in (col.profile.pii_findings if col.profile else [])] + [
                    g.uc_tag_value for g in (col.profile.governance_findings if col.profile else [])
                ]
                excluded_pii_notes.append(f"{logical_tname}.{col.name} ({', '.join(cats)})")
                continue

            # Skip standalone HHMMSS column when already stitched into companion date's _TS column
            if (
                col.profile
                and col.profile.legacy_date
                and col.profile.legacy_date.encoding == "hhmmss"
                and col.name in stitched_hhmmss
            ):
                continue

            sf_cname = sf_col_map[col.name]
            sf_type = col.snowflake_type or map_to_snowflake_type(col.raw_type, col.canonical_type)
            preferred_logical = col.semantic_name or col.name

            # 1. Native temporal columns OR legacy date columns (use Silver parsed _TS / _PARSED column!)
            if col.canonical_type in {"date", "timestamp"}:
                lname = _alloc_col_name(preferred_logical)
                col_raw_to_logical[col.name] = lname
                entry: dict[str, Any] = {
                    "name": lname,
                    "expr": _dq(sf_cname),
                    "data_type": sf_type,
                    "description": _clean_text(
                        col.business_description or col.comment or f"{_human_label(lname)} timestamp/date."
                    ),
                }
                syns = _synonyms_for_name(lname, col.name, col.semantic_name)
                if syns:
                    entry["synonyms"] = syns
                if col.is_primary_key and len(tbl.primary_key) == 1:
                    entry["unique"] = True
                    unique_cols_here.add(lname)
                time_dimensions.append(entry)
                continue

            if col.name in legacy_sf_map:
                derived_sf_col, derived_sf_type = legacy_sf_map[col.name]
                lname = _alloc_col_name(f"{preferred_logical}_ts" if derived_sf_type == "TIMESTAMP_NTZ" else preferred_logical)
                col_raw_to_logical[col.name] = lname
                entry = {
                    "name": lname,
                    "expr": _dq(derived_sf_col),
                    "data_type": derived_sf_type,
                    "description": _clean_text(
                        (col.business_description or col.comment or _human_label(preferred_logical))
                        + f" (parsed in Silver from legacy {col.profile.legacy_date.encoding} column {col.name})."  # type: ignore[union-attr]
                    ),
                }
                syns = _synonyms_for_name(lname, col.name, col.semantic_name)
                if syns:
                    entry["synonyms"] = syns
                time_dimensions.append(entry)
                continue

            # 2. Quantitative numeric measures -> facts
            if _is_numeric_measure_column(col, tbl):
                lname = _alloc_col_name(preferred_logical)
                col_raw_to_logical[col.name] = lname
                fact_entry: dict[str, Any] = {
                    "name": lname,
                    "expr": _dq(sf_cname),
                    "data_type": sf_type,
                    "description": _clean_text(
                        col.business_description or col.comment or f"{_human_label(lname)} numeric measure."
                    ),
                }
                syns = _synonyms_for_name(lname, col.name, col.semantic_name)
                if syns:
                    fact_entry["synonyms"] = syns
                facts.append(fact_entry)
                continue

            # 3. Qualitative attributes, keys, flags, codes -> dimensions
            lname = _alloc_col_name(preferred_logical)
            col_raw_to_logical[col.name] = lname
            desc = col.business_description or col.comment or f"{_human_label(lname)} attribute."
            if _is_direct_pii(col):
                desc += " [Sensitive identifier; masked under governance policy]"
            dim_entry: dict[str, Any] = {
                "name": lname,
                "expr": _dq(sf_cname),
                "data_type": sf_type,
                "description": _clean_text(desc),
            }
            syns = _synonyms_for_name(lname, col.name, col.semantic_name)
            if syns:
                dim_entry["synonyms"] = syns
            if (col.is_primary_key and len(tbl.primary_key) == 1) or col.is_unique:
                dim_entry["unique"] = True
                unique_cols_here.add(lname)
            sample_vals, is_enum = _safe_sample_values(col)
            if sample_vals:
                dim_entry["sample_values"] = sample_vals
                if is_enum:
                    dim_entry["is_enum"] = True
            dimensions.append(dim_entry)

            if col.profile and any(
                g.category == "plant_isolation_key" for g in col.profile.governance_findings
            ):
                plant_tables.append((logical_tname, lname))

        # Build table-level aggregated metrics
        seen_metric_names: set[str] = set(seen_col_names)

        def _alloc_metric_name(preferred: str) -> str:
            base = _slug(preferred)
            cand = base
            sfx = 2
            while cand in seen_metric_names:
                cand = f"{base}_{sfx}"
                sfx += 1
            seen_metric_names.add(cand)
            return cand

        # Record count metric
        single_pk_raw = tbl.primary_key[0] if len(tbl.primary_key) == 1 else None
        single_pk_sf = sf_col_map.get(single_pk_raw) if single_pk_raw else None
        count_name = _alloc_metric_name(
            "reading_count" if tbl.telemetry_profile else f"{logical_tname}_count"
        )
        metrics.append(
            {
                "name": count_name,
                "expr": f"COUNT(DISTINCT {_dq(single_pk_sf)})" if single_pk_sf else "COUNT(*)",
                "description": _clean_text(f"Total number of {_human_label(logical_tname).lower()} records."),
                "synonyms": [f"number of {logical_tname.replace('_', ' ')}", f"total {logical_tname.replace('_', ' ')}"],
            }
        )

        # Fact-based aggregated metrics (cap at top 8 facts so LLM context stays focused)
        fact_by_lname = {f["name"]: f for f in facts}
        for f_entry in facts[:8]:
            raw_col_obj = next(
                (c for c in tbl.columns if col_raw_to_logical.get(c.name) == f_entry["name"]),
                None,
            )
            agg = _pick_metric_aggregation(raw_col_obj) if raw_col_obj else "sum"
            if agg == "avg":
                m_name = _alloc_metric_name(f"avg_{f_entry['name']}")
                metrics.append(
                    {
                        "name": m_name,
                        "expr": f"AVG({f_entry['expr']})",
                        "description": _clean_text(f"Average {_human_label(f_entry['name']).lower()}."),
                    }
                )
                if tbl.telemetry_profile:
                    mx_name = _alloc_metric_name(f"max_{f_entry['name']}")
                    metrics.append(
                        {
                            "name": mx_name,
                            "expr": f"MAX({f_entry['expr']})",
                            "description": _clean_text(f"Maximum {_human_label(f_entry['name']).lower()}."),
                        }
                    )
            else:
                m_name = _alloc_metric_name(f"total_{f_entry['name']}")
                metrics.append(
                    {
                        "name": m_name,
                        "expr": f"SUM({f_entry['expr']})",
                        "description": _clean_text(f"Total {_human_label(f_entry['name']).lower()}."),
                    }
                )

        # Primary key columns mapped to logical names (only those emitted in dimensions/time_dimensions/facts)
        raw_pks = tbl.primary_key or (
            tbl.recommendations.primary_key_candidates[:1] if tbl.recommendations else []
        )
        logical_pks = [col_raw_to_logical[p] for p in raw_pks if p in col_raw_to_logical]
        if len(logical_pks) == 1:
            unique_cols_here.add(logical_pks[0])
            for entry_list in (dimensions, time_dimensions):
                for item in entry_list:
                    if item["name"] == logical_pks[0]:
                        item["unique"] = True

        role = tbl.recommendations.entity_role if tbl.recommendations else "unknown"
        t_desc = tbl.business_description or tbl.comment or (
            f"{_human_label(logical_tname)} ({role} table discovered from {tbl.full_name})."
        )
        if tbl.telemetry_profile:
            t_desc += f" OT telemetry ({tbl.telemetry_profile.pattern}); Silver filters out bad sensor quality readings."

        tbl_entry: dict[str, Any] = {
            "name": logical_tname,
            "description": _clean_text(t_desc),
            "base_table": {
                "database": db,
                "schema": silver_schema,
                "table": f"SILVER_{safe_sf_tbl}",
            },
        }
        t_syns = _synonyms_for_name(logical_tname, tbl.name, tbl.semantic_name)
        if t_syns:
            tbl_entry["synonyms"] = t_syns
        if logical_pks:
            tbl_entry["primary_key"] = {"columns": logical_pks}
        if dimensions:
            tbl_entry["dimensions"] = dimensions
        if time_dimensions:
            tbl_entry["time_dimensions"] = time_dimensions
        if facts:
            tbl_entry["facts"] = facts
        if metrics:
            tbl_entry["metrics"] = metrics

        semantic_tables.append(tbl_entry)
        raw_to_logical_col[tbl.full_name] = col_raw_to_logical
        unique_logical_cols[tbl.full_name] = unique_cols_here
        table_meta[tbl.full_name] = {
            "logical_name": logical_tname,
            "table": tbl,
            "dimensions": dimensions,
            "time_dimensions": time_dimensions,
            "facts": facts,
            "metrics": metrics,
            "fact_by_lname": fact_by_lname,
        }

    # Emit exploded Gold Hierarchy / BOM logical tables when detected
    hier_tables = get_hierarchy_tables(result)
    existing_logical_names = {t["name"] for t in semantic_tables}
    gold_hier_logical: list[tuple[str, TableInfo, bool]] = []
    for htbl in hier_tables:
        safe_h = sf_tbl_map[htbl.full_name]
        base_h_name = _slug(f"{logical_tbl_map.get(htbl.full_name, htbl.name)}_exploded")
        h_name = base_h_name
        sfx = 2
        while h_name in existing_logical_names:
            h_name = f"{base_h_name}_{sfx}"
            sfx += 1
        existing_logical_names.add(h_name)

        plant_obj = _find_plant_column(htbl)
        qty_obj = _find_quantity_column(htbl)
        h_col_map = deduplicate_snowflake_columns([c.name for c in htbl.columns])
        h_dims: list[dict[str, Any]] = []
        if plant_obj:
            p_sf = h_col_map[plant_obj.name]
            p_sf_type = plant_obj.snowflake_type or map_to_snowflake_type(
                plant_obj.raw_type, plant_obj.canonical_type
            )
            p_lname = _semantic_slug(plant_obj.semantic_name or plant_obj.name, suffix="col")
            if p_lname in {"root_node_id", "parent_node_id", "child_node_id", "hierarchy_path", "depth", "edge_qty", "extended_qty"}:
                p_lname = f"{p_lname}_plant"
            h_dims.append(
                {
                    "name": p_lname,
                    "expr": _dq(p_sf),
                    "data_type": p_sf_type,
                    "description": "Plant / site scoping key for multi-plant hierarchy isolation.",
                }
            )
        h_dims.extend(
            [
                {"name": "root_node_id", "expr": '"ROOT_NODE_ID"', "data_type": "VARCHAR", "description": "Top-level root assembly / parent node ID."},
                {"name": "parent_node_id", "expr": '"PARENT_NODE_ID"', "data_type": "VARCHAR", "description": "Direct parent node ID in the hierarchy edge."},
                {"name": "child_node_id", "expr": '"CHILD_NODE_ID"', "data_type": "VARCHAR", "description": "Direct child component / node ID."},
                {"name": "hierarchy_path", "expr": '"HIERARCHY_PATH"', "data_type": "VARCHAR", "description": "Full root-to-leaf path trace."},
            ]
        )
        h_facts: list[dict[str, Any]] = [
            {"name": "depth", "expr": '"DEPTH"', "data_type": "INTEGER", "description": "Hierarchy explosion level depth (1 = direct child)."}
        ]
        h_metrics: list[dict[str, Any]] = [
            {"name": "exploded_component_count", "expr": "COUNT(*)", "description": "Total exploded parent-child links across levels."},
            {"name": "max_hierarchy_depth", "expr": 'MAX("DEPTH")', "description": "Maximum nesting depth in the exploded tree."},
        ]
        if qty_obj:
            h_facts.extend(
                [
                    {"name": "edge_qty", "expr": '"EDGE_QTY"', "data_type": "FLOAT", "description": "Per-edge component quantity."},
                    {"name": "extended_qty", "expr": '"EXTENDED_QTY"', "data_type": "FLOAT", "description": "Cumulative rolled-up quantity from root assembly."},
                ]
            )
            h_metrics.append(
                {"name": "total_extended_qty", "expr": 'SUM("EXTENDED_QTY")', "description": "Total rolled-up component quantity across all levels."}
            )

        assert htbl.hierarchy is not None
        semantic_tables.append(
            {
                "name": h_name,
                "description": _clean_text(
                    f"Pre-exploded Gold recursive {htbl.hierarchy.pattern} table for {htbl.full_name} "
                    f"({htbl.hierarchy.parent_column} -> {htbl.hierarchy.child_column}). Use for multi-level rollups."
                ),
                "base_table": {
                    "database": db,
                    "schema": gold_schema,
                    "table": f"GOLD_{safe_h}_EXPLODED",
                },
                "dimensions": h_dims,
                "facts": h_facts,
                "metrics": h_metrics,
            }
        )
        gold_hier_logical.append((h_name, htbl, qty_obj is not None))

    # Build top-level relationships (requiring valid emitted logical columns + unique/PK right side)
    relationships_yaml: list[dict[str, Any]] = []
    seen_rel_names: set[str] = set()
    verified_rels = _filtered_verified_relationships(result, active_by_full)
    for rel in verified_rels:
        l_tbl = logical_tbl_map[rel.source_table]
        r_tbl = logical_tbl_map[rel.target_table]
        l_col = raw_to_logical_col.get(rel.source_table, {}).get(rel.source_column)
        r_col = raw_to_logical_col.get(rel.target_table, {}).get(rel.target_column)
        if not l_col or not r_col:
            continue
        # Cortex Analyst requires right_column of many_to_one join to be unique or PK on right_table
        if r_col not in unique_logical_cols.get(rel.target_table, set()):
            continue

        base_rname = _slug(f"{l_tbl}_to_{r_tbl}")
        rname = base_rname
        sfx = 2
        while rname in seen_rel_names:
            rname = f"{base_rname}_{sfx}"
            sfx += 1
        seen_rel_names.add(rname)

        rel_type = (
            "one_to_one"
            if l_col in unique_logical_cols.get(rel.source_table, set())
            else "many_to_one"
        )
        relationships_yaml.append(
            {
                "name": rname,
                "left_table": l_tbl,
                "right_table": r_tbl,
                "relationship_columns": [
                    {"left_column": l_col, "right_column": r_col}
                ],
                "join_type": "left_outer",
                "relationship_type": rel_type,
            }
        )

    # Synthesize grounded Verified Queries (VQRs)
    verified_queries = _build_cortex_verified_queries(
        active_tables=active_tables,
        table_meta=table_meta,
        relationships_yaml=relationships_yaml,
        gold_hier_logical=gold_hier_logical,
        plant_tables=plant_tables,
    )

    # Custom Instructions tailored to discovered signals
    instr_parts: list[str] = [
        f"This semantic model queries cleaned Silver Dynamic Tables in {db}.{silver_schema} discovered from {result.overview.database_name} ({result.overview.dialect})."
    ]
    if any(
        c.profile and c.profile.legacy_date
        for t in active_tables
        for c in t.columns
    ):
        instr_parts.append(
            "Legacy ERP/mainframe integer/Julian date columns have already been converted into DATE/TIMESTAMP_NTZ time dimensions in Silver; always filter and group using those time dimensions."
        )
    if any(t.telemetry_profile for t in active_tables):
        instr_parts.append(
            "OT historian and PLC telemetry Silver tables already exclude bad OPC quality readings; aggregate telemetry over hourly or daily buckets using DATE_TRUNC."
        )
    if gold_hier_logical:
        h_names = ", ".join(h[0] for h in gold_hier_logical)
        instr_parts.append(
            f"For multi-level BOM or hierarchy rollups, query the pre-exploded Gold logical table(s): {h_names}."
        )
    if plant_tables:
        p_info = ", ".join(f"{lt}.{lc}" for lt, lc in plant_tables[:4])
        instr_parts.append(
            f"Multi-plant data detected ({p_info}): always break down or filter by plant key when comparing facility performance."
        )
    if excluded_pii_notes:
        instr_parts.append(
            f"Sensitive PII/ITAR columns were intentionally excluded from this semantic model for governance compliance ({len(excluded_pii_notes)} column(s) omitted)."
        )
    if omitted_empty:
        instr_parts.append(
            f"{len(omitted_empty)} empty source table(s) (0 rows) were excluded from this semantic model."
        )

    model_doc: dict[str, Any] = {
        "name": f"{_slug(result.overview.database_name)}_semantic_model",
        "description": _clean_text(
            f"Auto-generated Snowflake Cortex Analyst semantic model for {result.overview.database_name} "
            f"({len(semantic_tables)} logical table(s), {len(relationships_yaml)} verified relationship(s))."
        ),
        "custom_instructions": " ".join(instr_parts),
        "tables": semantic_tables,
    }
    if relationships_yaml:
        model_doc["relationships"] = relationships_yaml
    if verified_queries:
        model_doc["verified_queries"] = verified_queries

    header = (
        f"# Auto-generated by tick: Snowflake Cortex Analyst Semantic Model for {result.overview.database_name}\n"
        f"# Upload to a Snowflake stage, e.g.:\n"
        f"#   CREATE STAGE IF NOT EXISTS {_dq(db)}.{_dq(silver_schema)}.\"SEMANTIC_STAGE\";\n"
        f"#   PUT file://cortex_analyst_semantic_model.yml @{db}.{silver_schema}.SEMANTIC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;\n\n"
    )
    return header + yaml.safe_dump(model_doc, sort_keys=False, width=110)


def _pick_best_fact_meta(
    active_tables: list[TableInfo],
    table_meta: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    scored: list[tuple[int, dict[str, Any]]] = []
    for tbl in active_tables:
        if tbl.telemetry_profile or tbl.hierarchy:
            continue
        meta = table_meta[tbl.full_name]
        role = tbl.recommendations.entity_role if tbl.recommendations else "unknown"
        score = 0
        if role == "fact":
            score += 500
        elif role == "event_log":
            score += 300
        if meta["facts"]:
            score += 200
        if meta["time_dimensions"]:
            score += 150
        if meta["dimensions"]:
            score += 50
        score += min(tbl.row_count or 0, 10_000) // 50
        scored.append((score, meta))
    if not scored:
        return None
    scored.sort(key=lambda x: -x[0])
    return scored[0][1]


def _build_cortex_verified_queries(
    active_tables: list[TableInfo],
    table_meta: dict[str, dict[str, Any]],
    relationships_yaml: list[dict[str, Any]],
    gold_hier_logical: list[tuple[str, TableInfo, bool]],
    plant_tables: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Synthesize grounded, Snowflake-valid Verified Queries (VQRs) using logical table/column names."""
    vqrs: list[dict[str, Any]] = []
    meta_by_logical = {m["logical_name"]: m for m in table_meta.values()}
    best_fact = _pick_best_fact_meta(active_tables, table_meta)

    # 1. Monthly trend on primary fact table (if time dimension exists)
    if best_fact and best_fact["time_dimensions"]:
        lt = best_fact["logical_name"]
        td = best_fact["time_dimensions"][0]["name"]
        fact_metric = best_fact["facts"][0]["name"] if best_fact["facts"] else None
        agg_sel = (
            f",\n  SUM({fact_metric}) AS total_{fact_metric}"
            if fact_metric
            else ""
        )
        vqrs.append(
            {
                "name": f"{lt}_monthly_trend",
                "question": f"What is the monthly trend of {_human_label(lt).lower()}"
                + (f" and total {_human_label(fact_metric).lower()}" if fact_metric else "")
                + "?",
                "sql": (
                    f"SELECT\n"
                    f"  DATE_TRUNC('MONTH', {td}) AS period_month,\n"
                    f"  COUNT(*) AS record_count{agg_sel}\n"
                    f"FROM {lt}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY 1 DESC\n"
                    f"LIMIT 24"
                ),
                "verified_by": "tick-auto-discovery",
                "use_as_onboarding_question": True,
            }
        )

    # 2. Joined fact + dimension breakdown (using first verified relationship from best fact or any rel)
    chosen_rel = next(
        (r for r in relationships_yaml if best_fact and r["left_table"] == best_fact["logical_name"]),
        relationships_yaml[0] if relationships_yaml else None,
    )
    if chosen_rel:
        lt = chosen_rel["left_table"]
        rt = chosen_rel["right_table"]
        lcol = chosen_rel["relationship_columns"][0]["left_column"]
        rcol = chosen_rel["relationship_columns"][0]["right_column"]
        lmeta = meta_by_logical.get(lt)
        rmeta = meta_by_logical.get(rt)
        if lmeta and rmeta:
            r_group_dim = next(
                (d["name"] for d in rmeta["dimensions"] if d["name"] != rcol and not d.get("unique")),
                rcol,
            )
            l_fact = lmeta["facts"][0]["name"] if lmeta["facts"] else None
            agg_sql = f",\n  SUM(l.{l_fact}) AS total_{l_fact}" if l_fact else ""
            vqrs.append(
                {
                    "name": f"{lt}_by_{rt}",
                    "question": f"Break down {_human_label(lt).lower()} by {_human_label(rt).lower()} {_human_label(r_group_dim).lower()}.",
                    "sql": (
                        f"SELECT\n"
                        f"  r.{r_group_dim},\n"
                        f"  COUNT(*) AS record_count{agg_sql}\n"
                        f"FROM {lt} AS l\n"
                        f"LEFT JOIN {rt} AS r\n"
                        f"  ON l.{lcol} = r.{rcol}\n"
                        f"GROUP BY 1\n"
                        f"ORDER BY record_count DESC\n"
                        f"LIMIT 25"
                    ),
                    "verified_by": "tick-auto-discovery",
                    "use_as_onboarding_question": True,
                }
            )
    elif best_fact and best_fact["dimensions"]:
        # Fallback breakdown by low-cardinality dimension
        lt = best_fact["logical_name"]
        low_dim = next(
            (d["name"] for d in best_fact["dimensions"] if d.get("sample_values")),
            best_fact["dimensions"][0]["name"],
        )
        l_fact = best_fact["facts"][0]["name"] if best_fact["facts"] else None
        agg_sql = f",\n  SUM({l_fact}) AS total_{l_fact}" if l_fact else ""
        vqrs.append(
            {
                "name": f"{lt}_by_{low_dim}",
                "question": f"Show {_human_label(lt).lower()} grouped by {_human_label(low_dim).lower()}.",
                "sql": (
                    f"SELECT\n"
                    f"  {low_dim},\n"
                    f"  COUNT(*) AS record_count{agg_sql}\n"
                    f"FROM {lt}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY record_count DESC\n"
                    f"LIMIT 25"
                ),
                "verified_by": "tick-auto-discovery",
                "use_as_onboarding_question": True,
            }
        )

    # 3. OT telemetry summary VQR
    ot_tbl = next((t for t in active_tables if t.telemetry_profile is not None), None)
    if ot_tbl:
        ometa = table_meta[ot_tbl.full_name]
        lt = ometa["logical_name"]
        ts_dim = ometa["time_dimensions"][0]["name"] if ometa["time_dimensions"] else None
        val_fact = ometa["facts"][0]["name"] if ometa["facts"] else None
        if ts_dim:
            avg_sql = f",\n  AVG({val_fact}) AS avg_{val_fact}" if val_fact else ""
            vqrs.append(
                {
                    "name": f"{lt}_hourly_telemetry",
                    "question": f"What is the hourly sensor reading volume"
                    + (f" and average {_human_label(val_fact).lower()}" if val_fact else "")
                    + f" for {_human_label(lt).lower()}?",
                    "sql": (
                        f"SELECT\n"
                        f"  DATE_TRUNC('HOUR', {ts_dim}) AS reading_hour,\n"
                        f"  COUNT(*) AS reading_count{avg_sql}\n"
                        f"FROM {lt}\n"
                        f"GROUP BY 1\n"
                        f"ORDER BY 1 DESC\n"
                        f"LIMIT 48"
                    ),
                    "verified_by": "tick-auto-discovery",
                    "use_as_onboarding_question": True,
                }
            )

    # 4. Exploded Gold BOM / Hierarchy VQR
    if gold_hier_logical:
        h_name, _, has_qty = gold_hier_logical[0]
        qty_sel = ",\n  SUM(extended_qty) AS total_extended_qty" if has_qty else ""
        vqrs.append(
            {
                "name": f"{h_name}_top_assemblies",
                "question": f"Which root assemblies have the most exploded components and deepest hierarchy in {_human_label(h_name).lower()}?",
                "sql": (
                    f"SELECT\n"
                    f"  root_node_id,\n"
                    f"  COUNT(*) AS exploded_component_count,\n"
                    f"  MAX(depth) AS max_hierarchy_depth{qty_sel}\n"
                    f"FROM {h_name}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY exploded_component_count DESC\n"
                    f"LIMIT 25"
                ),
                "verified_by": "tick-auto-discovery",
                "use_as_onboarding_question": True,
            }
        )

    # 5. Multi-plant comparison VQR (if plant key exists and wasn't already covered)
    if plant_tables:
        pt_tbl, pt_col = plant_tables[0]
        pmeta = meta_by_logical.get(pt_tbl)
        if pmeta:
            p_fact = pmeta["facts"][0]["name"] if pmeta["facts"] else None
            agg_sql = f",\n  SUM({p_fact}) AS total_{p_fact}" if p_fact else ""
            vqrs.append(
                {
                    "name": f"{pt_tbl}_by_plant",
                    "question": f"Compare {_human_label(pt_tbl).lower()} activity across plants.",
                    "sql": (
                        f"SELECT\n"
                        f"  {pt_col},\n"
                        f"  COUNT(*) AS record_count{agg_sql}\n"
                        f"FROM {pt_tbl}\n"
                        f"GROUP BY 1\n"
                        f"ORDER BY record_count DESC"
                    ),
                    "verified_by": "tick-auto-discovery",
                    "use_as_onboarding_question": False,
                }
            )

    return vqrs


# =============================================================================
# 2. Databricks Unity Catalog Metric Views SQL (version 1.1)
# =============================================================================


def _choose_metric_view_anchors(
    active_tables: list[TableInfo],
    verified_rels: list[RelationshipInfo],
) -> list[TableInfo]:
    """Choose the best tables to publish as Unity Catalog Metric Views (facts, telemetry, or top tables)."""
    outgoing_counts: dict[str, int] = {}
    for rel in verified_rels:
        outgoing_counts[rel.source_table] = outgoing_counts.get(rel.source_table, 0) + 1

    anchors: list[TableInfo] = []
    for tbl in active_tables:
        if tbl.hierarchy is not None:
            continue  # Handled via exploded Gold hierarchy metric view
        role = tbl.recommendations.entity_role if tbl.recommendations else "unknown"
        measure_count = sum(1 for c in tbl.columns if _is_numeric_measure_column(c, tbl))
        if (
            role in {"fact", "event_log", "bridge"}
            or tbl.telemetry_profile is not None
            or measure_count >= 2
            or (measure_count >= 1 and outgoing_counts.get(tbl.full_name, 0) > 0)
        ):
            anchors.append(tbl)

    # Fallback: if no classic facts/telemetry detected, pick up to 3 tables with measures or largest row count
    if not anchors and active_tables:
        non_hier = [t for t in active_tables if t.hierarchy is None] or active_tables
        ranked = sorted(
            non_hier,
            key=lambda t: (
                1 if any(_is_numeric_measure_column(c, t) for c in t.columns) else 0,
                t.row_count or 0,
                len(t.columns),
            ),
            reverse=True,
        )
        anchors = ranked[:3]

    return anchors


def render_databricks_metric_views_sql(
    result: DiscoveryResult,
    catalog: str | None = None,
    silver_schema: str = "silver",
    gold_schema: str = "gold",
) -> str:
    """Generate Databricks Unity Catalog Metric Views SQL (WITH METRICS LANGUAGE YAML, spec v1.1)."""
    cat = catalog or result.config.databricks_catalog
    all_base_tables = [t for t in result.tables if t.table_type == "table"]
    dbx_tbl_map = deduplicate_table_names(all_base_tables)
    active_tables, _ = _select_active_base_tables(result)
    active_by_full = {t.full_name: t for t in active_tables}
    logical_tbl_map = _deduplicate_logical_table_names(active_tables)
    verified_rels = _filtered_verified_relationships(result, active_by_full)
    join_keys = _collect_join_key_columns(result)

    lines: list[str] = [
        f"-- Auto-generated by tick: Databricks Unity Catalog Metric Views ({cat}.{gold_schema})",
        f"-- Source database: {result.overview.database_name} ({result.overview.dialect})",
        "-- Requires Databricks Runtime 16.4+ / Serverless SQL Warehouse with Unity Catalog Metric Views.",
        "",
        f"CREATE SCHEMA IF NOT EXISTS {_spark_ident(cat)}.{_spark_ident(gold_schema)};",
        "",
    ]

    anchors = _choose_metric_view_anchors(active_tables, verified_rels)
    hier_tables = get_hierarchy_tables(result)

    if not anchors and not hier_tables:
        lines.append("-- No eligible base tables discovered to generate Unity Catalog Metric Views.")
        return "\n".join(lines) + "\n"

    # Precompute Databricks Silver column maps for all active tables
    dbx_col_maps: dict[str, dict[str, str]] = {
        t.full_name: deduplicate_column_names([c.name for c in t.columns]) for t in active_tables
    }
    legacy_dbx_maps: dict[str, dict[str, tuple[str, str]]] = {
        t.full_name: _compute_databricks_silver_legacy_cols(t, dbx_col_maps[t.full_name])
        for t in active_tables
    }

    seen_view_names: set[str] = set()

    def _alloc_view_name(base_slug: str) -> str:
        base = f"mv_{_slug(base_slug)}"
        cand = base
        sfx = 2
        while cand in seen_view_names:
            cand = f"{base}_{sfx}"
            sfx += 1
        seen_view_names.add(cand)
        return cand

    for tbl in anchors:
        safe_tbl = dbx_tbl_map[tbl.full_name]
        logical_tname = logical_tbl_map[tbl.full_name]
        view_name = _alloc_view_name(logical_tname)
        view_fqn = _spark_table_ref(cat, gold_schema, view_name)
        src_fqn = _spark_table_ref(cat, silver_schema, f"silver_{safe_tbl}")
        col_map = dbx_col_maps[tbl.full_name]
        legacy_map = legacy_dbx_maps[tbl.full_name]

        # Outgoing many-to-one joins for this anchor table (up to 6 unique dimension joins)
        outgoing = [r for r in verified_rels if r.source_table == tbl.full_name]
        joins_yaml: list[dict[str, Any]] = []
        joined_tables: list[tuple[str, TableInfo]] = []
        seen_join_aliases: set[str] = {"source"}
        seen_join_targets: set[str] = set()

        for rel in outgoing:
            if len(joins_yaml) >= 6:
                break
            if rel.target_table in seen_join_targets:
                continue
            tgt_tbl = active_by_full[rel.target_table]
            tgt_safe = dbx_tbl_map[tgt_tbl.full_name]
            tgt_col_map = dbx_col_maps[tgt_tbl.full_name]
            src_col_dbx = col_map.get(rel.source_column)
            tgt_col_dbx = tgt_col_map.get(rel.target_column)
            if not src_col_dbx or not tgt_col_dbx:
                continue

            base_alias = _slug(logical_tbl_map[tgt_tbl.full_name])
            alias = base_alias
            sfx = 2
            while alias in seen_join_aliases:
                alias = f"{base_alias}_{sfx}"
                sfx += 1
            seen_join_aliases.add(alias)
            seen_join_targets.add(rel.target_table)

            joins_yaml.append(
                {
                    "name": alias,
                    "source": _spark_table_ref(cat, silver_schema, f"silver_{tgt_safe}"),
                    "on": f"source.{_spark_ident(src_col_dbx)} = {_spark_ident(alias)}.{_spark_ident(tgt_col_dbx)}",
                }
            )
            joined_tables.append((alias, tgt_tbl))

        # Dimensions & Measures
        dimensions_yaml: list[dict[str, Any]] = []
        measures_yaml: list[dict[str, Any]] = []
        seen_dim_names: set[str] = set()
        seen_meas_names: set[str] = set()

        def _alloc_dim(preferred: str) -> str:
            base = _semantic_slug(preferred, suffix="col")
            cand = base
            sfx = 2
            while cand in seen_dim_names:
                cand = f"{base}_{sfx}"
                sfx += 1
            seen_dim_names.add(cand)
            return cand

        def _alloc_meas(preferred: str) -> str:
            base = _semantic_slug(preferred, suffix="metric")
            cand = base
            sfx = 2
            while cand in seen_meas_names or cand in seen_dim_names:
                cand = f"{base}_{sfx}"
                sfx += 1
            seen_meas_names.add(cand)
            return cand

        stitched_hhmmss = {
            c.profile.legacy_date.paired_column
            for c in tbl.columns
            if c.profile and c.profile.legacy_date and c.profile.legacy_date.paired_column
        }

        primary_time_expr: str | None = None
        primary_time_slug: str | None = None
        measure_cols: list[tuple[ColumnInfo, str]] = []

        for col in tbl.columns:
            if col.canonical_type == "binary":
                continue
            is_join_or_pk = col.is_primary_key or (tbl.full_name, col.name) in join_keys
            if (_is_direct_pii(col) or _is_itar_or_ip(col)) and not is_join_or_pk:
                continue
            if (
                col.profile
                and col.profile.legacy_date
                and col.profile.legacy_date.encoding == "hhmmss"
                and col.name in stitched_hhmmss
            ):
                continue

            dbx_col = col_map[col.name]
            pref_name = col.semantic_name or col.name

            # Legacy parsed date/timestamp column in Silver
            if col.name in legacy_map:
                derived_col, _ = legacy_map[col.name]
                dname = _alloc_dim(pref_name)
                expr = f"source.{_spark_ident(derived_col)}"
                dimensions_yaml.append(
                    {
                        "name": dname,
                        "expr": expr,
                        "display_name": _human_label(dname),
                        "comment": _clean_text(
                            col.business_description or col.comment or f"Parsed date/time from {col.name}"
                        ),
                    }
                )
                if primary_time_expr is None:
                    primary_time_expr = expr
                    primary_time_slug = dname
                continue

            # Native date / timestamp column
            if col.canonical_type in {"date", "timestamp"}:
                dname = _alloc_dim(pref_name)
                expr = f"source.{_spark_ident(dbx_col)}"
                dimensions_yaml.append(
                    {
                        "name": dname,
                        "expr": expr,
                        "display_name": _human_label(dname),
                        "comment": _clean_text(
                            col.business_description or col.comment or f"{_human_label(dname)} date/timestamp"
                        ),
                    }
                )
                if primary_time_expr is None:
                    primary_time_expr = expr
                    primary_time_slug = dname
                continue

            # Numeric measures
            if _is_numeric_measure_column(col, tbl):
                measure_cols.append((col, dbx_col))
                continue

            # Regular dimension (skip if already super wide > 20 source dimensions)
            if len(dimensions_yaml) < 20:
                dname = _alloc_dim(pref_name)
                dim_obj: dict[str, Any] = {
                    "name": dname,
                    "expr": f"source.{_spark_ident(dbx_col)}",
                    "display_name": _human_label(dname),
                }
                if col.business_description or col.comment:
                    dim_obj["comment"] = _clean_text(col.business_description or col.comment or "")
                dimensions_yaml.append(dim_obj)

        # Add convenient calendar grain dimension for primary time column
        if primary_time_expr and primary_time_slug:
            grain_unit = "HOUR" if tbl.telemetry_profile else "MONTH"
            grain_suffix = "hour" if tbl.telemetry_profile else "month"
            gname = _alloc_dim(f"{primary_time_slug}_{grain_suffix}")
            dimensions_yaml.append(
                {
                    "name": gname,
                    "expr": f"DATE_TRUNC('{grain_unit}', {primary_time_expr})",
                    "display_name": f"{_human_label(primary_time_slug)} ({grain_suffix.title()})",
                    "comment": f"{grain_suffix.title()} time bucket derived from {primary_time_slug}.",
                }
            )

        # Bring in top non-PII descriptive dimensions from joined dimension tables (up to 4 per join)
        for j_alias, j_tbl in joined_tables:
            j_col_map = dbx_col_maps[j_tbl.full_name]
            added_from_join = 0
            for jcol in j_tbl.columns:
                if added_from_join >= 4:
                    break
                if jcol.canonical_type not in {"string", "boolean"}:
                    continue
                if jcol.is_primary_key or jcol.is_foreign_key:
                    continue
                if _is_direct_pii(jcol) or _is_itar_or_ip(jcol):
                    continue
                j_dbx = j_col_map[jcol.name]
                j_pref = f"{j_alias}_{jcol.semantic_name or jcol.name}"
                dname = _alloc_dim(j_pref)
                dim_obj = {
                    "name": dname,
                    "expr": f"{_spark_ident(j_alias)}.{_spark_ident(j_dbx)}",
                    "display_name": _human_label(dname),
                }
                if jcol.business_description or jcol.comment:
                    dim_obj["comment"] = _clean_text(jcol.business_description or jcol.comment or "")
                dimensions_yaml.append(dim_obj)
                added_from_join += 1

        # Build Measures
        cnt_name = _alloc_meas("reading_count" if tbl.telemetry_profile else "record_count")
        measures_yaml.append(
            {
                "name": cnt_name,
                "expr": "COUNT(1)",
                "display_name": _human_label(cnt_name),
                "comment": _clean_text(f"Total rows in {_human_label(logical_tname)}."),
            }
        )
        if len(tbl.primary_key) == 1:
            pk_raw = tbl.primary_key[0]
            if pk_raw in col_map:
                pk_dbx = col_map[pk_raw]
                dpk_name = _alloc_meas(f"distinct_{logical_tname}")
                measures_yaml.append(
                    {
                        "name": dpk_name,
                        "expr": f"COUNT(DISTINCT source.{_spark_ident(pk_dbx)})",
                        "display_name": f"Distinct {_human_label(logical_tname)}",
                        "comment": _clean_text(f"Distinct count of primary key {pk_dbx}."),
                    }
                )

        for mcol, mdbx in measure_cols[:8]:
            m_slug = _slug(mcol.semantic_name or mcol.name)
            agg = _pick_metric_aggregation(mcol)
            col_ref = f"source.{_spark_ident(mdbx)}"
            if agg == "avg":
                mname = _alloc_meas(f"avg_{m_slug}")
                m_obj: dict[str, Any] = {
                    "name": mname,
                    "expr": f"AVG({col_ref})",
                    "display_name": f"Avg {_human_label(m_slug)}",
                    "comment": _clean_text(mcol.business_description or mcol.comment or f"Average {_human_label(m_slug)}."),
                }
            else:
                mname = _alloc_meas(f"total_{m_slug}")
                m_obj = {
                    "name": mname,
                    "expr": f"SUM({col_ref})",
                    "display_name": f"Total {_human_label(m_slug)}",
                    "comment": _clean_text(mcol.business_description or mcol.comment or f"Sum of {_human_label(m_slug)}."),
                }
            fmt = _databricks_measure_format(mcol)
            if fmt:
                m_obj["format"] = fmt
            measures_yaml.append(m_obj)

            if tbl.telemetry_profile:
                mx_name = _alloc_meas(f"max_{m_slug}")
                measures_yaml.append(
                    {
                        "name": mx_name,
                        "expr": f"MAX({col_ref})",
                        "display_name": f"Max {_human_label(m_slug)}",
                        "comment": _clean_text(f"Maximum {_human_label(m_slug)} reading."),
                    }
                )

        mv_yaml: dict[str, Any] = {
            "version": 1.1,
            "comment": _clean_text(
                tbl.business_description
                or tbl.comment
                or f"Governed Unity Catalog Metric View for {tbl.full_name} ({logical_tname})."
            ),
            "source": src_fqn,
        }
        if joins_yaml:
            mv_yaml["joins"] = joins_yaml
        if dimensions_yaml:
            mv_yaml["dimensions"] = dimensions_yaml
        mv_yaml["measures"] = measures_yaml

        yaml_body = yaml.safe_dump(mv_yaml, sort_keys=False, width=110).replace("$$", "$ $").rstrip()
        sample_dim = dimensions_yaml[0]["name"] if dimensions_yaml else None
        sample_meas = measures_yaml[-1]["name"] if len(measures_yaml) > 1 else measures_yaml[0]["name"]

        lines.extend(
            [
                f"-- Metric View for {tbl.full_name}" + (f" ({tbl.semantic_name})" if tbl.semantic_name else ""),
                f"CREATE OR REPLACE VIEW {view_fqn}",
                "WITH METRICS",
                "LANGUAGE YAML",
                "AS $$",
                yaml_body,
                "$$;",
                "",
            ]
        )
        if sample_dim:
            lines.append(
                f"-- Example query: SELECT {_spark_ident(sample_dim)}, MEASURE({_spark_ident(sample_meas)}) "
                f"FROM {view_fqn} GROUP BY 1 ORDER BY 2 DESC LIMIT 20;"
            )
            lines.append("")

    # Emit Metric Views for Gold Recursive Hierarchy / BOM exploded tables
    for htbl in hier_tables:
        assert htbl.hierarchy is not None
        safe_h = dbx_tbl_map[htbl.full_name]
        logical_h = logical_tbl_map.get(htbl.full_name, _slug(htbl.name))
        view_name = _alloc_view_name(f"{logical_h}_bom")
        view_fqn = _spark_table_ref(cat, gold_schema, view_name)
        gold_src_fqn = _spark_table_ref(cat, gold_schema, f"gold_{safe_h}_exploded")

        h_col_map = deduplicate_column_names([c.name for c in htbl.columns])
        plant_obj = _find_plant_column(htbl)
        qty_obj = _find_quantity_column(htbl)

        h_dims: list[dict[str, Any]] = []
        if plant_obj:
            p_dbx = h_col_map[plant_obj.name]
            p_dname = _semantic_slug(plant_obj.semantic_name or plant_obj.name, suffix="col")
            if p_dname in {"root_node_id", "parent_node_id", "child_node_id", "depth"}:
                p_dname = f"{p_dname}_plant"
            h_dims.append(
                {
                    "name": p_dname,
                    "expr": f"source.{_spark_ident(p_dbx)}",
                    "display_name": "Plant",
                }
            )
        h_dims.extend(
            [
                {"name": "root_node_id", "expr": "source.root_node_id", "display_name": "Root Assembly"},
                {"name": "parent_node_id", "expr": "source.parent_node_id", "display_name": "Parent Component"},
                {"name": "child_node_id", "expr": "source.child_node_id", "display_name": "Child Component"},
                {"name": "depth", "expr": "source.depth", "display_name": "BOM Depth"},
            ]
        )
        h_measures: list[dict[str, Any]] = [
            {
                "name": "exploded_component_links",
                "expr": "COUNT(1)",
                "display_name": "Exploded Component Links",
                "comment": "Total exploded parent-child BOM edges.",
            },
            {
                "name": "max_bom_depth",
                "expr": "MAX(source.depth)",
                "display_name": "Max Hierarchy Depth",
                "comment": "Deepest nesting level under each root node.",
            },
        ]
        if qty_obj:
            h_measures.append(
                {
                    "name": "total_extended_qty",
                    "expr": "SUM(source.extended_qty)",
                    "display_name": "Total Extended Quantity",
                    "comment": "Rolled-up extended component quantity across all hierarchy levels.",
                }
            )

        hmv_yaml: dict[str, Any] = {
            "version": 1.1,
            "comment": _clean_text(
                f"Gold recursive {htbl.hierarchy.pattern} rollup Metric View for {htbl.full_name}."
            ),
            "source": gold_src_fqn,
            "dimensions": h_dims,
            "measures": h_measures,
        }
        yaml_body = yaml.safe_dump(hmv_yaml, sort_keys=False, width=110).replace("$$", "$ $").rstrip()
        lines.extend(
            [
                f"-- Gold Recursive Hierarchy Metric View for {htbl.full_name} ({htbl.hierarchy.pattern})",
                f"CREATE OR REPLACE VIEW {view_fqn}",
                "WITH METRICS",
                "LANGUAGE YAML",
                "AS $$",
                yaml_body,
                "$$;",
                f"-- Example query: SELECT root_node_id, MEASURE(exploded_component_links), MEASURE(max_bom_depth) "
                f"FROM {view_fqn} GROUP BY 1 ORDER BY 2 DESC LIMIT 20;",
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _databricks_measure_format(col: ColumnInfo) -> dict[str, Any] | None:
    name_to_check = col.semantic_name or col.name
    if _CURRENCY_MEASURE_RE.search(name_to_check) or _CURRENCY_MEASURE_RE.search(col.name):
        return {"type": "currency", "currency_code": "USD", "decimal_places": {"type": "exact", "places": 2}}
    if _PERCENT_MEASURE_RE.search(name_to_check) or _PERCENT_MEASURE_RE.search(col.name):
        return {"type": "percentage", "decimal_places": {"type": "exact", "places": 1}}
    if col.canonical_type in {"decimal", "float"}:
        return {"type": "number", "decimal_places": {"type": "exact", "places": 2}}
    return None


# =============================================================================
# 3. Databricks AI/BI Genie Space Context & Knowledge Store Guide
# =============================================================================


def render_databricks_genie_context_md(
    result: DiscoveryResult,
    catalog: str | None = None,
    bronze_schema: str | None = None,
    silver_schema: str = "silver",
    gold_schema: str = "gold",
) -> str:
    """Render a ready-to-paste Databricks AI/BI Genie Space setup, instructions, joins, and benchmark guide."""
    cat = catalog or result.config.databricks_catalog
    bsch = bronze_schema or result.config.databricks_schema
    all_base_tables = [t for t in result.tables if t.table_type == "table"]
    dbx_tbl_map = deduplicate_table_names(all_base_tables)
    active_tables, omitted_empty = _select_active_base_tables(result)
    active_by_full = {t.full_name: t for t in active_tables}
    logical_tbl_map = _deduplicate_logical_table_names(active_tables)
    verified_rels = _filtered_verified_relationships(result, active_by_full)
    anchors = _choose_metric_view_anchors(active_tables, verified_rels)
    hier_tables = get_hierarchy_tables(result)

    dbx_col_maps: dict[str, dict[str, str]] = {
        t.full_name: deduplicate_column_names([c.name for c in t.columns]) for t in active_tables
    }
    legacy_dbx_maps: dict[str, dict[str, tuple[str, str]]] = {
        t.full_name: _compute_databricks_silver_legacy_cols(t, dbx_col_maps[t.full_name])
        for t in active_tables
    }

    lines: list[str] = [
        f"# Databricks AI/BI Genie Space Blueprint: `{result.overview.database_name}`",
        "",
        f"- **Source System:** `{result.overview.dialect}` ({result.overview.database_name})",
        f"- **Target Unity Catalog:** `{cat}` (Bronze: `{bsch}` · Silver: `{silver_schema}` · Gold: `{gold_schema}`)",
        f"- **Active Data Assets:** `{len(active_tables)}` Silver tables · `{len(anchors) + len(hier_tables)}` Metric Views · `{len(verified_rels)}` verified joins",
        "",
        "---",
        "",
        "## 1. Recommended Unity Catalog Assets to Add to Genie Space",
        "",
        "Add these governed **Silver tables**, **Gold exploded tables**, and **Gold Metric Views** (do **not** attach raw Bronze tables):",
        "",
        "| Layer | Unity Catalog Asset | Business Name | Role | Notes |",
        "|---|---|---|---|---|",
    ]

    for tbl in active_tables:
        safe_tbl = dbx_tbl_map[tbl.full_name]
        fqn = f"{cat}.{silver_schema}.silver_{safe_tbl}"
        role = tbl.recommendations.entity_role if tbl.recommendations else "unknown"
        notes: list[str] = []
        if tbl.telemetry_profile:
            notes.append(f"OT {tbl.telemetry_profile.pattern}")
        if legacy_dbx_maps[tbl.full_name]:
            notes.append("Legacy dates parsed")
        if tbl.hierarchy:
            notes.append(f"Use Gold exploded view for multi-level {tbl.hierarchy.pattern}")
        lines.append(
            f"| Silver Table | `{_escape_md(fqn)}` | `{_escape_md(tbl.semantic_name or tbl.name)}` | "
            f"`{role}` | {_escape_md('; '.join(notes) or (tbl.business_description or tbl.comment))} |"
        )

    seen_mv_slugs: set[str] = set()
    for tbl in anchors:
        base = f"mv_{_slug(logical_tbl_map[tbl.full_name])}"
        mv_name = base
        sfx = 2
        while mv_name in seen_mv_slugs:
            mv_name = f"{base}_{sfx}"
            sfx += 1
        seen_mv_slugs.add(mv_name)
        lines.append(
            f"| Gold Metric View | `{_escape_md(f'{cat}.{gold_schema}.{mv_name}')}` | "
            f"`{_escape_md(logical_tbl_map[tbl.full_name])}` KPIs | `metric_view` | "
            f"Governed `MEASURE(...)` definitions (`metric_views.sql`) |"
        )

    for htbl in hier_tables:
        safe_h = dbx_tbl_map[htbl.full_name]
        logical_h = logical_tbl_map.get(htbl.full_name, _slug(htbl.name))
        lines.append(
            f"| Gold Hierarchy | `{_escape_md(f'{cat}.{gold_schema}.gold_{safe_h}_exploded')}` | "
            f"`{_escape_md(htbl.semantic_name or htbl.name)}` exploded | `hierarchy` | "
            f"Pre-exploded recursive {htbl.hierarchy.pattern if htbl.hierarchy else 'tree'} |"
        )
        base_hmv = f"mv_{_slug(f'{logical_h}_bom')}"
        hmv_name = base_hmv
        sfx = 2
        while hmv_name in seen_mv_slugs:
            hmv_name = f"{base_hmv}_{sfx}"
            sfx += 1
        seen_mv_slugs.add(hmv_name)
        lines.append(
            f"| Gold Metric View | `{_escape_md(f'{cat}.{gold_schema}.{hmv_name}')}` | "
            f"`{_escape_md(logical_h)}` BOM KPIs | `metric_view` | "
            f"Governed recursive rollup `MEASURE(...)` definitions (`metric_views.sql`) |"
        )

    if omitted_empty:
        lines.append("")
        lines.append(
            f"> **Excluded Empty Tables ({len(omitted_empty)}):** "
            + ", ".join(f"`{t.full_name}`" for t in omitted_empty[:15])
            + (" ..." if len(omitted_empty) > 15 else "")
        )

    # 2. Copy-Paste Genie General Instructions
    lines.extend(
        [
            "",
            "---",
            "",
            "## 2. Copy-Paste Genie General Instructions",
            "",
            "Paste the block below into **Genie Space → Configure → Instructions → Text**:",
            "",
            "```text",
            f"You are an analytics assistant for {result.overview.database_name} data stored in Unity Catalog `{cat}`.",
            f"1. ALWAYS query cleaned Silver tables in `{cat}.{silver_schema}` or Gold Metric Views in `{cat}.{gold_schema}` — NEVER query raw Bronze tables in `{cat}.{bsch}`.",
            f"2. When answering KPI or aggregation questions, prefer Unity Catalog Metric Views (`{cat}.{gold_schema}.mv_*`) using MEASURE(<metric_name>) so calculations remain consistent across dashboards.",
        ]
    )

    step_no = 3
    glossary_pairs = [
        f"{t.name} = {t.semantic_name}"
        for t in active_tables
        if t.semantic_name and t.semantic_name.lower() != t.name.lower()
    ]
    col_glossary_pairs: list[str] = []
    seen_cg: set[str] = set()
    for t in active_tables:
        for c in t.columns:
            if c.semantic_name and c.semantic_name.lower() != c.name.lower():
                pair = f"{c.name} = {c.semantic_name}"
                if pair not in seen_cg:
                    seen_cg.add(pair)
                    col_glossary_pairs.append(pair)
    if glossary_pairs or col_glossary_pairs:
        terms = ", ".join((glossary_pairs[:8] + col_glossary_pairs[:10]))
        lines.append(f"{step_no}. ERP/MES Business Glossary mappings: {terms}.")
        step_no += 1

    legacy_notes = [
        f"silver_{dbx_tbl_map[t.full_name]}.{derived_col} (from {raw_col})"
        for t in active_tables
        for raw_col, (derived_col, _) in legacy_dbx_maps[t.full_name].items()
    ]
    if legacy_notes:
        lines.append(
            f"{step_no}. Legacy date/time fields are pre-parsed in Silver — use {', '.join(legacy_notes[:8])} instead of raw integer/Julian codes."
        )
        step_no += 1

    ot_tables = [t for t in active_tables if t.telemetry_profile is not None]
    if ot_tables:
        ot_list = ", ".join(f"silver_{dbx_tbl_map[t.full_name]}" for t in ot_tables)
        lines.append(
            f"{step_no}. OT Telemetry ({ot_list}): Silver Lakeflow expectations flag/filter bad OPC sensor quality. Bucket time-series queries with DATE_TRUNC('hour', ...) or DATE_TRUNC('day', ...)."
        )
        step_no += 1

    if hier_tables:
        h_list = ", ".join(f"`{cat}.{gold_schema}.gold_{dbx_tbl_map[h.full_name]}_exploded`" for h in hier_tables)
        lines.append(
            f"{step_no}. Multi-level BOM / hierarchy queries: ALWAYS use {h_list} (`root_node_id`, `child_node_id`, `depth`, `extended_qty`) rather than writing recursive CTEs ad hoc."
        )
        step_no += 1

    pii_cols_list = [
        f"{dbx_tbl_map[t.full_name]}.{dbx_col_maps[t.full_name][c.name]}"
        for t in active_tables
        for c in t.columns
        if _is_direct_pii(c) or _is_itar_or_ip(c)
    ]
    if pii_cols_list:
        lines.append(
            f"{step_no}. Governance & Privacy: NEVER display raw PII or export-controlled IP columns ({', '.join(pii_cols_list[:10])}); rely on Unity Catalog column masks and aggregate results."
        )
        step_no += 1

    plant_cols_list = [
        f"{dbx_tbl_map[t.full_name]}.{dbx_col_maps[t.full_name][c.name]}"
        for t in active_tables
        for c in t.columns
        if c.profile and any(g.category == "plant_isolation_key" for g in c.profile.governance_findings)
    ]
    if plant_cols_list:
        lines.append(
            f"{step_no}. Multi-Plant Scoping: Include or filter by plant isolation keys ({', '.join(plant_cols_list[:6])}) when comparing facilities."
        )

    lines.append("```")

    # 3. Verified Join Definitions for Genie Space
    lines.extend(["", "---", "", "## 3. Verified Join Paths (for Genie Space Joins)", ""])
    if verified_rels:
        lines.append("| Left Silver Table | Right Silver Table | Join Condition (`ON`) | Kind | Confidence |")
        lines.append("|---|---|---|---|---:|")
        for rel in verified_rels:
            l_safe = dbx_tbl_map[rel.source_table]
            r_safe = dbx_tbl_map[rel.target_table]
            l_col = dbx_col_maps[rel.source_table].get(rel.source_column, sanitize_delta_column_name(rel.source_column))
            r_col = dbx_col_maps[rel.target_table].get(rel.target_column, sanitize_delta_column_name(rel.target_column))
            on_expr = f"silver_{l_safe}.{_spark_ident(l_col)} = silver_{r_safe}.{_spark_ident(r_col)}"
            lines.append(
                f"| `silver_{_escape_md(l_safe)}` | `silver_{_escape_md(r_safe)}` | `{_escape_md(on_expr)}` | "
                f"`{rel.kind}` | {rel.confidence:.0%} |"
            )
    else:
        lines.append("_No explicit or high-confidence cross-table joins discovered._")

    # 4. Safe Low-Cardinality Filter Value Dictionary (strictly PII-free!)
    filter_rows: list[str] = []
    for tbl in active_tables:
        safe_tbl = dbx_tbl_map[tbl.full_name]
        for col in tbl.columns:
            vals, is_enum = _safe_sample_values(col, limit=8)
            if vals:
                dbx_c = dbx_col_maps[tbl.full_name][col.name]
                val_str = ", ".join(f"`{_escape_md(v)}`" for v in vals)
                enum_tag = "Exact enum" if is_enum else "Top values"
                filter_rows.append(
                    f"| `silver_{_escape_md(safe_tbl)}` | `{_escape_md(dbx_c)}` | "
                    f"{_escape_md(col.semantic_name or col.name)} | {enum_tag} | {val_str} |"
                )

    lines.extend(["", "---", "", "## 4. Safe Filter Value Reference (PII & Sensitive Columns Excluded)", ""])
    if filter_rows:
        lines.append("| Silver Table | Column | Business Term | Coverage | Safe Filter Values |")
        lines.append("|---|---|---|---|---|")
        lines.extend(filter_rows[:40])
    else:
        lines.append("_No low-cardinality non-sensitive filter columns profiled._")

    # 5. Starter Benchmark Questions & SQL Expressions
    lines.extend(["", "---", "", "## 5. Starter Sample Questions & Benchmark SQL", ""])
    benchmarks = _build_genie_benchmark_queries(
        cat=cat,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        active_tables=active_tables,
        anchors=anchors,
        hier_tables=hier_tables,
        dbx_tbl_map=dbx_tbl_map,
        dbx_col_maps=dbx_col_maps,
        legacy_dbx_maps=legacy_dbx_maps,
        logical_tbl_map=logical_tbl_map,
        verified_rels=verified_rels,
    )
    for idx, (question, sql_text) in enumerate(benchmarks, start=1):
        lines.extend(
            [
                f"### Q{idx}: {question}",
                "",
                "```sql",
                sql_text,
                "```",
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _build_genie_benchmark_queries(
    cat: str,
    silver_schema: str,
    gold_schema: str,
    active_tables: list[TableInfo],
    anchors: list[TableInfo],
    hier_tables: list[TableInfo],
    dbx_tbl_map: dict[str, str],
    dbx_col_maps: dict[str, dict[str, str]],
    legacy_dbx_maps: dict[str, dict[str, tuple[str, str]]],
    logical_tbl_map: dict[str, str],
    verified_rels: list[RelationshipInfo],
) -> list[tuple[str, str]]:
    queries: list[tuple[str, str]] = []

    if anchors:
        top_tbl = anchors[0]
        mv_name = f"mv_{_slug(logical_tbl_map[top_tbl.full_name])}"
        mv_fqn = _spark_table_ref(cat, gold_schema, mv_name)
        # Recompute exact emitted dimension/measure names for top_tbl so benchmark SQL matches metric_views.sql
        top_dims: list[str] = []
        seen_top_dims: set[str] = set()
        top_measures: list[tuple[ColumnInfo, str]] = []
        stitched_top = {
            c.profile.legacy_date.paired_column
            for c in top_tbl.columns
            if c.profile and c.profile.legacy_date and c.profile.legacy_date.paired_column
        }
        for col in top_tbl.columns:
            if col.canonical_type == "binary":
                continue
            if (_is_direct_pii(col) or _is_itar_or_ip(col)) and not col.is_primary_key:
                continue
            if (
                col.profile
                and col.profile.legacy_date
                and col.profile.legacy_date.encoding == "hhmmss"
                and col.name in stitched_top
            ):
                continue
            if _is_numeric_measure_column(col, top_tbl):
                top_measures.append((col, _slug(col.semantic_name or col.name)))
                continue
            if len(top_dims) < 20:
                base_d = _semantic_slug(col.semantic_name or col.name, suffix="col")
                cand_d = base_d
                sfx = 2
                while cand_d in seen_top_dims:
                    cand_d = f"{base_d}_{sfx}"
                    sfx += 1
                seen_top_dims.add(cand_d)
                top_dims.append(cand_d)

        count_m = "reading_count" if top_tbl.telemetry_profile else "record_count"
        if count_m in seen_top_dims:
            sfx = 2
            while f"{count_m}_{sfx}" in seen_top_dims:
                sfx += 1
            count_m = f"{count_m}_{sfx}"

        if top_measures:
            m_col, m_slug = top_measures[0]
            base_m = f"{'avg' if _pick_metric_aggregation(m_col) == 'avg' else 'total'}_{m_slug}"
            cand_m = _semantic_slug(base_m, suffix="metric")
            sfx = 2
            while cand_m in seen_top_dims or cand_m == count_m:
                cand_m = f"{base_m}_{sfx}"
                sfx += 1
            m_expr = f"MEASURE({_spark_ident(cand_m)})"
        else:
            m_expr = f"MEASURE({_spark_ident(count_m)})"

        if top_dims:
            # Prefer a non-PK string/boolean dimension if present, else first dimension
            pref_dim = next(
                (
                    _semantic_slug(c.semantic_name or c.name, suffix="col")
                    for c in top_tbl.columns
                    if c.canonical_type in {"string", "boolean"}
                    and not _is_direct_pii(c)
                    and not _is_itar_or_ip(c)
                    and not c.is_primary_key
                    and _semantic_slug(c.semantic_name or c.name, suffix="col") in seen_top_dims
                ),
                top_dims[0],
            )
            queries.append(
                (
                    f"What is the breakdown of {_human_label(logical_tbl_map[top_tbl.full_name]).lower()} by {_human_label(pref_dim).lower()}?",
                    f"SELECT {_spark_ident(pref_dim)}, {m_expr} AS metric_value\n"
                    f"FROM {mv_fqn}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY 2 DESC\n"
                    f"LIMIT 20;",
                )
            )
        else:
            queries.append(
                (
                    f"What is the overall {_human_label(logical_tbl_map[top_tbl.full_name]).lower()} summary?",
                    f"SELECT {m_expr} AS metric_value\nFROM {mv_fqn};",
                )
            )

    # Joined Silver query if a verified relationship exists
    if verified_rels:
        rel = verified_rels[0]
        l_safe = dbx_tbl_map[rel.source_table]
        r_safe = dbx_tbl_map[rel.target_table]
        l_fqn = _spark_table_ref(cat, silver_schema, f"silver_{l_safe}")
        r_fqn = _spark_table_ref(cat, silver_schema, f"silver_{r_safe}")
        l_col = dbx_col_maps[rel.source_table][rel.source_column]
        r_col = dbx_col_maps[rel.target_table][rel.target_column]
        queries.append(
            (
                f"How many {_human_label(l_safe).lower()} records match each {_human_label(r_safe).lower()}?",
                f"SELECT r.{_spark_ident(r_col)}, COUNT(*) AS record_count\n"
                f"FROM {l_fqn} AS l\n"
                f"LEFT JOIN {r_fqn} AS r\n"
                f"  ON l.{_spark_ident(l_col)} = r.{_spark_ident(r_col)}\n"
                f"GROUP BY 1\n"
                f"ORDER BY record_count DESC\n"
                f"LIMIT 25;",
            )
        )

    # OT Telemetry benchmark query
    ot_tbl = next((t for t in active_tables if t.telemetry_profile is not None), None)
    if ot_tbl and ot_tbl.telemetry_profile:
        safe_ot = dbx_tbl_map[ot_tbl.full_name]
        ot_fqn = _spark_table_ref(cat, silver_schema, f"silver_{safe_ot}")
        ts_c = dbx_col_maps[ot_tbl.full_name].get(
            ot_tbl.telemetry_profile.timestamp_column,
            sanitize_delta_column_name(ot_tbl.telemetry_profile.timestamp_column),
        )
        val_raw = ot_tbl.telemetry_profile.value_columns[0] if ot_tbl.telemetry_profile.value_columns else None
        val_c = dbx_col_maps[ot_tbl.full_name].get(val_raw) if val_raw else None
        avg_part = f", AVG({_spark_ident(val_c)}) AS avg_{val_c}" if val_c else ""
        queries.append(
            (
                f"Show hourly sensor reading volume for {_human_label(safe_ot).lower()}.",
                f"SELECT DATE_TRUNC('HOUR', {_spark_ident(ts_c)}) AS reading_hour,\n"
                f"       COUNT(*) AS reading_count{avg_part}\n"
                f"FROM {ot_fqn}\n"
                f"GROUP BY 1\n"
                f"ORDER BY 1 DESC\n"
                f"LIMIT 48;",
            )
        )

    # Gold BOM benchmark query
    if hier_tables:
        ht = hier_tables[0]
        safe_h = dbx_tbl_map[ht.full_name]
        gold_fqn = _spark_table_ref(cat, gold_schema, f"gold_{safe_h}_exploded")
        has_qty = _find_quantity_column(ht) is not None
        qty_part = ", SUM(extended_qty) AS total_extended_qty" if has_qty else ""
        queries.append(
            (
                f"Which root assemblies have the deepest and largest exploded BOM in {_human_label(safe_h).lower()}?",
                f"SELECT root_node_id, COUNT(*) AS component_links, MAX(depth) AS max_depth{qty_part}\n"
                f"FROM {gold_fqn}\n"
                f"GROUP BY 1\n"
                f"ORDER BY component_links DESC\n"
                f"LIMIT 25;",
            )
        )

    # Fallback if no queries generated (e.g. empty schema)
    if not queries and active_tables:
        t0 = active_tables[0]
        safe_0 = dbx_tbl_map[t0.full_name]
        fqn_0 = _spark_table_ref(cat, silver_schema, f"silver_{safe_0}")
        queries.append(
            (
                f"How many rows are in {_human_label(safe_0).lower()}?",
                f"SELECT COUNT(*) AS total_rows\nFROM {fqn_0};",
            )
        )

    return queries


def export_semantic_bundle(
    result: DiscoveryResult,
    output_dir: str | Path,
    databricks_catalog: str | None = None,
    databricks_bronze_schema: str | None = None,
    databricks_silver_schema: str = "silver",
    databricks_gold_schema: str = "gold",
    snowflake_database: str | None = None,
    snowflake_silver_schema: str = "SILVER",
    snowflake_gold_schema: str = "GOLD",
) -> list[Path]:
    """Write AI / Semantic Layer artifacts for the configured target platform(s) into output_dir."""
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    target_plat = result.config.target_platform

    if target_plat in {"databricks", "both"}:
        mv_path = root / "metric_views.sql"
        mv_path.write_text(
            render_databricks_metric_views_sql(
                result,
                catalog=databricks_catalog,
                silver_schema=databricks_silver_schema,
                gold_schema=databricks_gold_schema,
            ),
            encoding="utf-8",
        )
        written.append(mv_path)

        genie_path = root / "genie_context.md"
        genie_path.write_text(
            render_databricks_genie_context_md(
                result,
                catalog=databricks_catalog,
                bronze_schema=databricks_bronze_schema,
                silver_schema=databricks_silver_schema,
                gold_schema=databricks_gold_schema,
            ),
            encoding="utf-8",
        )
        written.append(genie_path)

    if target_plat in {"snowflake", "both"}:
        cortex_path = root / "cortex_analyst_semantic_model.yml"
        cortex_path.write_text(
            render_cortex_analyst_yaml(
                result,
                database=snowflake_database,
                silver_schema=snowflake_silver_schema,
                gold_schema=snowflake_gold_schema,
            ),
            encoding="utf-8",
        )
        written.append(cortex_path)

    return written
