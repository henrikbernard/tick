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

# Strong semantic measure hints
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

# Percentage/rate measures (checked BEFORE currency so tax_rate / profit_margin_pct format as %)
_PERCENT_MEASURE_RE = re.compile(
    r"(?:^|_)(?:pct|percent|percentage|rate|ratio|oee|yield)(?:$|_)",
    re.I,
)

# Currency-like measures (used for Databricks Metric View format hints)
_CURRENCY_MEASURE_RE = re.compile(
    r"(?:^|_)(?:price|cost|rev|revenue|sales|profit|amount|amt|total_amount|net_value|netwr|"
    r"stprs|verpr|spend|budget|tax|fee|discount|balance|subtotal|gross|margin)(?:$|_)",
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
    "lateral", "left", "like", "limit", "localtime", "localtimestamp", "match_recognize", "merge",
    "natural", "not", "null", "offset", "on", "or", "order", "outer", "over", "partition",
    "preceding", "primary", "qualify", "range", "regexp", "revoke", "right", "rlike", "row",
    "rows", "sample", "select", "set", "some", "start", "table", "tablesample", "then", "to",
    "true", "try_cast", "union", "unique", "update", "user", "using", "values", "view", "when",
    "where", "window", "with",
}


def _slug(val: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", val.strip().lower()).strip("_") or "item"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned


def _semantic_slug(val: str, suffix: str = "col") -> str:
    """Clean lowercase identifier that avoids hard SQL parser-breaking reserved words."""
    s = _slug(val)
    if s in _HARD_SQL_RESERVED:
        return f"{s}_{suffix}"
    return s


def _human_label(name: str) -> str:
    cleaned = re.sub(r"[_\-.]+", " ", name.strip()).strip()
    return " ".join(w.capitalize() for w in cleaned.split()) or name


def _clean_text(val: str) -> str:
    """Normalize whitespace and separate any consecutive '$' runs that could confuse dollar-quoting."""
    separated = re.sub(r"\${2,}", lambda m: " ".join(m.group(0)), val)
    return re.sub(r"\s+", " ", separated).strip()


def _pick_dollar_quote_tag(body: str) -> str:
    """Return '$$' if safe, otherwise an unused tagged dollar-quote delimiter (e.g. '$mv_yaml$')."""
    if "$$" not in body:
        return "$$"
    base = "mv_yaml"
    tag = f"${base}$"
    sfx = 2
    while tag in body:
        tag = f"${base}_{sfx}$"
        sfx += 1
    return tag


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


def _md_code(val: str | None) -> str:
    """Wrap text in a Markdown inline code span using a backtick fence wider than any inner backticks."""
    if not val:
        return "—"
    escaped = _escape_md(val)
    max_run = max((len(m.group(0)) for m in re.finditer(r"`+", escaped)), default=0)
    fence = "`" * (max_run + 1)
    if max_run > 0 or escaped.startswith("`") or escaped.endswith("`"):
        return f"{fence} {escaped} {fence}"
    return f"{fence}{escaped}{fence}"


def _md_fence(content: str) -> str:
    max_ticks = max((len(m.group(0)) for m in re.finditer(r"`+", content)), default=0)
    return "`" * max(3, max_ticks + 1)


def _is_direct_pii(col: ColumnInfo) -> bool:
    return bool(col.profile and col.profile.pii_findings)


def _is_itar_or_ip(col: ColumnInfo) -> bool:
    return bool(
        col.profile
        and any(g.category == "ip_or_export_controlled" for g in col.profile.governance_findings)
    )


def _is_plant_key(col: ColumnInfo) -> bool:
    return bool(
        col.profile
        and any(g.category == "plant_isolation_key" for g in col.profile.governance_findings)
    )


def _is_sensitive_for_samples(col: ColumnInfo) -> bool:
    return bool(col.profile and (col.profile.pii_findings or col.profile.governance_findings))


def _effective_pk_columns(tbl: TableInfo) -> list[str]:
    """Return explicit PK columns, or single heuristic PK candidate when no explicit PK exists."""
    if tbl.primary_key:
        return list(tbl.primary_key)
    if tbl.recommendations and tbl.recommendations.primary_key_candidates:
        return list(tbl.recommendations.primary_key_candidates[:1])
    return []


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


def _is_numeric_measure_column(
    col: ColumnInfo,
    tbl: TableInfo,
    join_keys: set[tuple[str, str]] | None = None,
    pk_cols: set[str] | None = None,
) -> bool:
    """Determine if a numeric column is a quantitative fact/measure rather than a key, code, PII, or date."""
    if col.canonical_type not in {"decimal", "float", "integer"}:
        return False
    if col.is_primary_key or col.is_foreign_key:
        return False
    if pk_cols and col.name in pk_cols:
        return False
    if join_keys and (tbl.full_name, col.name.lower()) in join_keys:
        return False
    if _is_direct_pii(col) or _is_itar_or_ip(col) or _is_plant_key(col):
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
    # Reject identifier/code/flag/year names BEFORE checking broad measure hints (e.g. tax_id, cost_center_code)
    if _NON_MEASURE_NUM_RE.search(name_to_check) or _NON_MEASURE_NUM_RE.search(col.name):
        return False
    if _MEASURE_HINT_RE.search(name_to_check) or _MEASURE_HINT_RE.search(col.name):
        return True
    if col.canonical_type in {"decimal", "float"}:
        return True
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
) -> dict[str, tuple[str, str, bool]]:
    """Mirror snowflake_export.py legacy derivation: raw_col -> (sf_col_name, sf_type, is_date_or_ts)."""
    emitted_cols: set[str] = set(sf_col_map.values())
    derived: dict[str, tuple[str, str, bool]] = {}
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
            if ld.encoding == "hhmmss":
                # Unpaired HHMMSS produces zero-padded 6-char VARCHAR in Silver, NOT DATE
                derived[col.name] = (target_col, "VARCHAR", False)
            else:
                sf_type = "TIMESTAMP_NTZ" if ld.paired_column else "DATE"
                derived[col.name] = (target_col, sf_type, True)
    return derived


def _compute_databricks_silver_legacy_cols(
    tbl: TableInfo,
    dbx_col_map: dict[str, str],
) -> dict[str, tuple[str, str, bool]]:
    """Mirror databricks_export.py legacy derivation: raw_col -> (dbx_col_name, spark_type, is_date_or_ts)."""
    seen_stitched: set[str] = set(dbx_col_map.values())
    derived: dict[str, tuple[str, str, bool]] = {}
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
            if ld.encoding == "hhmmss":
                derived[col.name] = (target_col, "STRING", False)
            else:
                spark_type = "TIMESTAMP" if ld.paired_column else "DATE"
                derived[col.name] = (target_col, spark_type, True)
    return derived


def _collect_join_key_columns(result: DiscoveryResult, min_confidence: float = 0.75) -> set[tuple[str, str]]:
    """Return {(table_full_name, lower_column_name)} participating in verified relationships."""
    keys: set[tuple[str, str]] = set()
    for rel in result.relationships:
        if rel.kind == "explicit_fk" or rel.confidence >= min_confidence:
            keys.add((rel.source_table, rel.source_column.lower()))
            keys.add((rel.target_table, rel.target_column.lower()))
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


def _is_unique_single_key(tbl: TableInfo, col_name: str) -> bool:
    """True if col_name uniquely identifies rows in tbl (single-col PK, is_unique, unique 1-col index, or PK candidate)."""
    col_low = col_name.lower()
    if len(tbl.primary_key) == 1 and tbl.primary_key[0].lower() == col_low:
        return True
    for c in tbl.columns:
        if c.name.lower() == col_low and c.is_unique:
            return True
    for idx in tbl.indexes:
        if idx.unique and len(idx.columns) == 1 and idx.columns[0].lower() == col_low:
            return True
    if (
        not tbl.primary_key
        and tbl.recommendations
        and tbl.recommendations.primary_key_candidates
        and tbl.recommendations.primary_key_candidates[0].lower() == col_low
    ):
        return True
    return False


def _is_unique_column_set(tbl: TableInfo, col_names: list[str]) -> bool:
    """True if col_names matches the full primary key or a unique index on tbl."""
    if not col_names:
        return False
    if len(col_names) == 1:
        return _is_unique_single_key(tbl, col_names[0])
    target_set = {c.lower() for c in col_names}
    if tbl.primary_key and {p.lower() for p in tbl.primary_key} == target_set and len(tbl.primary_key) == len(col_names):
        return True
    for idx in tbl.indexes:
        if idx.unique and {c.lower() for c in idx.columns} == target_set and len(idx.columns) == len(col_names):
            return True
    return False


def _resolve_col_name_ci(col_map: dict[str, str], raw_col: str) -> str | None:
    """Look up raw_col in col_map (exact first, then case-insensitive)."""
    if raw_col in col_map:
        return col_map[raw_col]
    raw_low = raw_col.lower()
    for k, v in col_map.items():
        if k.lower() == raw_low:
            return v
    return None


def _resolve_referred_table(
    src_tbl: TableInfo,
    referred_schema: str | None,
    referred_table: str,
    active_by_full: dict[str, TableInfo],
) -> TableInfo | None:
    cand_schema = referred_schema or src_tbl.schema_name
    cand_full = f"{cand_schema}.{referred_table}"
    if cand_full in active_by_full:
        return active_by_full[cand_full]
    low_full = cand_full.lower()
    for full, tbl in active_by_full.items():
        if full.lower() == low_full:
            return tbl
    matches = [t for t in active_by_full.values() if t.name.lower() == referred_table.lower()]
    return matches[0] if len(matches) == 1 else None


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

    raw_to_logical_col: dict[str, dict[str, str]] = {}
    unique_logical_cols: dict[str, set[str]] = {}
    table_meta: dict[str, dict[str, Any]] = {}
    excluded_pii_notes: list[str] = []
    plant_tables: list[tuple[str, str]] = []

    semantic_tables: list[dict[str, Any]] = []

    for tbl in active_tables:
        logical_tname = logical_tbl_map[tbl.full_name]
        safe_sf_tbl = sf_tbl_map[tbl.full_name]
        sf_col_map = deduplicate_snowflake_columns([c.name for c in tbl.columns])
        legacy_sf_map = _compute_snowflake_silver_legacy_cols(tbl, sf_col_map)
        eff_pks = _effective_pk_columns(tbl)
        eff_pk_set = set(eff_pks)

        dimensions: list[dict[str, Any]] = []
        time_dimensions: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []
        metrics: list[dict[str, Any]] = []

        seen_col_names: set[str] = set()
        col_raw_to_logical: dict[str, str] = {}
        keyable_logical_cols: set[str] = set()  # dimensions + time_dimensions only (valid for PK/FK)
        unique_cols_here: set[str] = set()
        non_pk_unique_cols: list[str] = []
        fact_agg_by_lname: dict[str, str] = {}

        def _alloc_col_name(preferred: str) -> str:
            base = _semantic_slug(preferred, suffix="col")
            cand = base
            sfx = 2
            while cand in seen_col_names:
                cand = f"{base}_{sfx}"
                sfx += 1
            seen_col_names.add(cand)
            return cand

        stitched_hhmmss = {
            c.profile.legacy_date.paired_column
            for c in tbl.columns
            if c.profile and c.profile.legacy_date and c.profile.legacy_date.paired_column
        }

        for col in tbl.columns:
            if col.canonical_type == "binary":
                continue

            is_join_or_pk = (
                col.is_primary_key
                or col.name in eff_pk_set
                or (tbl.full_name, col.name.lower()) in join_keys
            )

            # Exclude direct PII & ITAR/IP columns unless needed as PK/join key
            if (_is_direct_pii(col) or _is_itar_or_ip(col)) and not is_join_or_pk:
                cats = [f.category for f in (col.profile.pii_findings if col.profile else [])] + [
                    g.uc_tag_value for g in (col.profile.governance_findings if col.profile else [])
                ]
                excluded_pii_notes.append(f"{logical_tname}.{col.name} ({', '.join(cats)})")
                continue

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

            # 1a. Native temporal columns -> time_dimensions
            if col.canonical_type in {"date", "timestamp"}:
                lname = _alloc_col_name(preferred_logical)
                col_raw_to_logical[col.name] = lname
                keyable_logical_cols.add(lname)
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
                if (col.is_primary_key and len(tbl.primary_key) == 1) or col.is_unique:
                    entry["unique"] = True
                    unique_cols_here.add(lname)
                    if not (col.is_primary_key and len(tbl.primary_key) == 1):
                        non_pk_unique_cols.append(lname)
                time_dimensions.append(entry)
                continue

            # 1b. Legacy date/time columns: DATE/TIMESTAMP -> time_dimension; unpaired HHMMSS -> VARCHAR dimension
            if col.name in legacy_sf_map:
                derived_sf_col, derived_sf_type, is_temporal = legacy_sf_map[col.name]
                if is_temporal:
                    lname = _alloc_col_name(
                        f"{preferred_logical}_ts" if derived_sf_type == "TIMESTAMP_NTZ" else preferred_logical
                    )
                    col_raw_to_logical[col.name] = lname
                    keyable_logical_cols.add(lname)
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
                else:
                    lname = _alloc_col_name(preferred_logical)
                    col_raw_to_logical[col.name] = lname
                    keyable_logical_cols.add(lname)
                    entry = {
                        "name": lname,
                        "expr": _dq(derived_sf_col),
                        "data_type": derived_sf_type,
                        "description": _clean_text(
                            (col.business_description or col.comment or _human_label(preferred_logical))
                            + f" (zero-padded HHMMSS string parsed from {col.name})."
                        ),
                    }
                    syns = _synonyms_for_name(lname, col.name, col.semantic_name)
                    if syns:
                        entry["synonyms"] = syns
                    dimensions.append(entry)
                continue

            # 2. Quantitative numeric measures -> facts
            if _is_numeric_measure_column(col, tbl, join_keys=join_keys, pk_cols=eff_pk_set):
                lname = _alloc_col_name(preferred_logical)
                col_raw_to_logical[col.name] = lname
                fact_agg_by_lname[lname] = _pick_metric_aggregation(col)
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
            keyable_logical_cols.add(lname)
            desc = col.business_description or col.comment or f"{_human_label(lname)} attribute."
            if _is_direct_pii(col) or _is_itar_or_ip(col):
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
                if not (col.is_primary_key and len(tbl.primary_key) == 1):
                    non_pk_unique_cols.append(lname)
            sample_vals, is_enum = _safe_sample_values(col)
            if sample_vals:
                dim_entry["sample_values"] = sample_vals
                if is_enum:
                    dim_entry["is_enum"] = True
            dimensions.append(dim_entry)

            if _is_plant_key(col):
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

        for f_entry in facts[:8]:
            agg = fact_agg_by_lname.get(f_entry["name"], "sum")
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

        # Primary key: emit ONLY if ALL key columns were emitted as keyable dimensions/time_dimensions
        logical_pks = [
            col_raw_to_logical[p]
            for p in eff_pks
            if p in col_raw_to_logical and col_raw_to_logical[p] in keyable_logical_cols
        ]
        emit_pks = logical_pks if (eff_pks and len(logical_pks) == len(eff_pks)) else []
        if len(emit_pks) == 1:
            unique_cols_here.add(emit_pks[0])
            for entry_list in (dimensions, time_dimensions):
                for item in entry_list:
                    if item["name"] == emit_pks[0]:
                        item["unique"] = True
            non_pk_unique_cols = [u for u in non_pk_unique_cols if u != emit_pks[0]]

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
        if emit_pks:
            tbl_entry["primary_key"] = {"columns": emit_pks}
        if non_pk_unique_cols:
            tbl_entry["unique_keys"] = [{"columns": [u]} for u in dict.fromkeys(non_pk_unique_cols)]
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
            "fact_agg_by_lname": fact_agg_by_lname,
            "raw_to_logical": col_raw_to_logical,
        }

    # Emit exploded Gold Hierarchy / BOM logical tables for active hierarchy tables
    hier_tables = [t for t in get_hierarchy_tables(result) if t.full_name in active_by_full]
    existing_logical_names = {t["name"] for t in semantic_tables}
    gold_hier_logical: list[tuple[str, TableInfo, bool]] = []
    for htbl in hier_tables:
        assert htbl.hierarchy is not None
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
        parent_obj = next((c for c in htbl.columns if c.name == htbl.hierarchy.parent_column), None)
        child_obj = next((c for c in htbl.columns if c.name == htbl.hierarchy.child_column), None)
        p_node_type = (
            parent_obj.snowflake_type or map_to_snowflake_type(parent_obj.raw_type, parent_obj.canonical_type)
            if parent_obj
            else "VARCHAR"
        )
        c_node_type = (
            child_obj.snowflake_type or map_to_snowflake_type(child_obj.raw_type, child_obj.canonical_type)
            if child_obj
            else p_node_type
        )
        root_node_type = p_node_type if p_node_type == c_node_type else "VARCHAR"

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
                {"name": "root_node_id", "expr": '"ROOT_NODE_ID"', "data_type": root_node_type, "description": "Top-level root assembly / parent node ID."},
                {"name": "parent_node_id", "expr": '"PARENT_NODE_ID"', "data_type": p_node_type, "description": "Direct parent node ID in the hierarchy edge."},
                {"name": "child_node_id", "expr": '"CHILD_NODE_ID"', "data_type": c_node_type, "description": "Direct child component / node ID."},
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
        l_col = _resolve_col_name_ci(raw_to_logical_col.get(rel.source_table, {}), rel.source_column)
        r_col = _resolve_col_name_ci(raw_to_logical_col.get(rel.target_table, {}), rel.target_column)
        if not l_col or not r_col:
            continue
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
        c.profile and c.profile.legacy_date and c.profile.legacy_date.encoding != "hhmmss"
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

    stage_ref = f"{_dq(db)}.{_dq(silver_schema)}.\"SEMANTIC_STAGE\""
    header = (
        f"# Auto-generated by tick: Snowflake Cortex Analyst Semantic Model for {result.overview.database_name}\n"
        f"# Upload to a Snowflake stage, e.g.:\n"
        f"#   CREATE STAGE IF NOT EXISTS {stage_ref};\n"
        f"#   PUT file://cortex_analyst_semantic_model.yml @{stage_ref} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;\n\n"
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


def _vqr_fact_agg_clause(meta: dict[str, Any], prefix: str = "") -> tuple[str, str]:
    """Return (sql_select_fragment, question_phrase) respecting AVG vs SUM for the table's first fact."""
    if not meta["facts"]:
        return "", ""
    fname = meta["facts"][0]["name"]
    agg = meta["fact_agg_by_lname"].get(fname, "sum")
    col_ref = f"{prefix}{fname}"
    if agg == "avg":
        return f",\n  AVG({col_ref}) AS avg_{fname}", f" and average {_human_label(fname).lower()}"
    return f",\n  SUM({col_ref}) AS total_{fname}", f" and total {_human_label(fname).lower()}"


def _build_cortex_verified_queries(
    active_tables: list[TableInfo],
    table_meta: dict[str, dict[str, Any]],
    relationships_yaml: list[dict[str, Any]],
    gold_hier_logical: list[tuple[str, TableInfo, bool]],
    plant_tables: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Synthesize grounded, Snowflake-valid Verified Queries (VQRs) using logical table/column names."""
    vqrs: list[dict[str, Any]] = []
    seen_vqr_names: set[str] = set()
    covered_group_pairs: set[tuple[str, str]] = set()
    meta_by_logical = {m["logical_name"]: m for m in table_meta.values()}
    best_fact = _pick_best_fact_meta(active_tables, table_meta)

    def _alloc_vqr_name(base: str) -> str:
        cand = _slug(base)
        sfx = 2
        while cand in seen_vqr_names:
            cand = f"{_slug(base)}_{sfx}"
            sfx += 1
        seen_vqr_names.add(cand)
        return cand

    # 1. Monthly trend on primary fact table (if time dimension exists)
    if best_fact and best_fact["time_dimensions"]:
        lt = best_fact["logical_name"]
        td = best_fact["time_dimensions"][0]["name"]
        agg_sel, q_phrase = _vqr_fact_agg_clause(best_fact)
        month_alias = "period_month" if td != "period_month" else "bucket_month"
        cnt_alias = "record_count" if td != "record_count" else "row_cnt"
        vqrs.append(
            {
                "name": _alloc_vqr_name(f"{lt}_monthly_trend"),
                "question": f"What is the monthly trend of {_human_label(lt).lower()}{q_phrase}?",
                "sql": (
                    f"SELECT\n"
                    f"  DATE_TRUNC('MONTH', {td}) AS {month_alias},\n"
                    f"  COUNT(*) AS {cnt_alias}{agg_sel}\n"
                    f"FROM {lt}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY 1 DESC\n"
                    f"LIMIT 24"
                ),
                "verified_by": "tick-auto-discovery",
                "use_as_onboarding_question": True,
            }
        )

    # 2. Joined fact + dimension breakdown (or fallback low-cardinality dimension breakdown)
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
            agg_sql, _ = _vqr_fact_agg_clause(lmeta, prefix="l.")
            cnt_alias = "record_count" if r_group_dim != "record_count" else "row_cnt"
            vqrs.append(
                {
                    "name": _alloc_vqr_name(f"{lt}_by_{rt}"),
                    "question": f"Break down {_human_label(lt).lower()} by {_human_label(rt).lower()} {_human_label(r_group_dim).lower()}.",
                    "sql": (
                        f"SELECT\n"
                        f"  r.{r_group_dim},\n"
                        f"  COUNT(*) AS {cnt_alias}{agg_sql}\n"
                        f"FROM {lt} AS l\n"
                        f"LEFT JOIN {rt} AS r\n"
                        f"  ON l.{lcol} = r.{rcol}\n"
                        f"GROUP BY 1\n"
                        f"ORDER BY {cnt_alias} DESC\n"
                        f"LIMIT 25"
                    ),
                    "verified_by": "tick-auto-discovery",
                    "use_as_onboarding_question": True,
                }
            )
    elif best_fact and best_fact["dimensions"]:
        lt = best_fact["logical_name"]
        low_dim = next(
            (d["name"] for d in best_fact["dimensions"] if d.get("sample_values")),
            best_fact["dimensions"][0]["name"],
        )
        agg_sql, _ = _vqr_fact_agg_clause(best_fact)
        cnt_alias = "record_count" if low_dim != "record_count" else "row_cnt"
        covered_group_pairs.add((lt, low_dim))
        vqrs.append(
            {
                "name": _alloc_vqr_name(f"{lt}_by_{low_dim}"),
                "question": f"Show {_human_label(lt).lower()} grouped by {_human_label(low_dim).lower()}.",
                "sql": (
                    f"SELECT\n"
                    f"  {low_dim},\n"
                    f"  COUNT(*) AS {cnt_alias}{agg_sql}\n"
                    f"FROM {lt}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY {cnt_alias} DESC\n"
                    f"LIMIT 25"
                ),
                "verified_by": "tick-auto-discovery",
                "use_as_onboarding_question": True,
            }
        )

    # 3. OT telemetry summary VQR (explicitly resolves telemetry_profile.timestamp_column!)
    ot_tbl = next((t for t in active_tables if t.telemetry_profile is not None), None)
    if ot_tbl and ot_tbl.telemetry_profile:
        ometa = table_meta[ot_tbl.full_name]
        lt = ometa["logical_name"]
        ot_ts_logical = ometa["raw_to_logical"].get(ot_tbl.telemetry_profile.timestamp_column)
        td_by_name = {d["name"]: d for d in ometa["time_dimensions"]}
        ts_entry = td_by_name.get(ot_ts_logical) if ot_ts_logical else None
        if ts_entry is None:
            ts_entry = next(
                (d for d in ometa["time_dimensions"] if "TIMESTAMP" in d["data_type"].upper()),
                ometa["time_dimensions"][0] if ometa["time_dimensions"] else None,
            )
        val_fact = ometa["facts"][0]["name"] if ometa["facts"] else None
        if ts_entry:
            ts_dim = ts_entry["name"]
            is_ts = "TIMESTAMP" in ts_entry["data_type"].upper()
            trunc_unit = "HOUR" if is_ts else "DAY"
            bucket_label = "hourly" if is_ts else "daily"
            bucket_alias = ("reading_hour" if is_ts else "reading_day")
            if bucket_alias == ts_dim:
                bucket_alias = f"{bucket_alias}_bucket"
            avg_sql = f",\n  AVG({val_fact}) AS avg_{val_fact}" if val_fact else ""
            vqrs.append(
                {
                    "name": _alloc_vqr_name(f"{lt}_{bucket_label}_telemetry"),
                    "question": f"What is the {bucket_label} sensor reading volume"
                    + (f" and average {_human_label(val_fact).lower()}" if val_fact else "")
                    + f" for {_human_label(lt).lower()}?",
                    "sql": (
                        f"SELECT\n"
                        f"  DATE_TRUNC('{trunc_unit}', {ts_dim}) AS {bucket_alias},\n"
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
                "name": _alloc_vqr_name(f"{h_name}_top_assemblies"),
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

    # 5. Multi-plant comparison VQR (skip any (table, col) already covered by VQR #2)
    for pt_tbl, pt_col in plant_tables:
        if (pt_tbl, pt_col) in covered_group_pairs:
            continue
        pmeta = meta_by_logical.get(pt_tbl)
        if not pmeta:
            continue
        agg_sql, _ = _vqr_fact_agg_clause(pmeta)
        cnt_alias = "record_count" if pt_col != "record_count" else "row_cnt"
        covered_group_pairs.add((pt_tbl, pt_col))
        vqrs.append(
            {
                "name": _alloc_vqr_name(f"{pt_tbl}_by_plant"),
                "question": f"Compare {_human_label(pt_tbl).lower()} activity across plants.",
                "sql": (
                    f"SELECT\n"
                    f"  {pt_col},\n"
                    f"  COUNT(*) AS {cnt_alias}{agg_sql}\n"
                    f"FROM {pt_tbl}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY {cnt_alias} DESC"
                ),
                "verified_by": "tick-auto-discovery",
                "use_as_onboarding_question": False,
            }
        )
        break

    return vqrs


# =============================================================================
# 2. Databricks Unity Catalog Metric Views SQL (version 1.1)
# =============================================================================


def _choose_metric_view_anchors(
    active_tables: list[TableInfo],
    verified_rels: list[RelationshipInfo],
    join_keys: set[tuple[str, str]],
) -> list[TableInfo]:
    """Choose the best tables to publish as Unity Catalog Metric Views (facts, telemetry, or top tables)."""
    outgoing_counts: dict[str, int] = {}
    for rel in verified_rels:
        outgoing_counts[rel.source_table] = outgoing_counts.get(rel.source_table, 0) + 1

    anchors: list[TableInfo] = []
    for tbl in active_tables:
        if tbl.hierarchy is not None:
            continue
        role = tbl.recommendations.entity_role if tbl.recommendations else "unknown"
        pk_set = set(_effective_pk_columns(tbl))
        measure_count = sum(
            1 for c in tbl.columns if _is_numeric_measure_column(c, tbl, join_keys=join_keys, pk_cols=pk_set)
        )
        if (
            role in {"fact", "event_log", "bridge"}
            or tbl.telemetry_profile is not None
            or measure_count >= 2
            or (measure_count >= 1 and outgoing_counts.get(tbl.full_name, 0) > 0)
        ):
            anchors.append(tbl)

    if not anchors and active_tables:
        non_hier = [t for t in active_tables if t.hierarchy is None] or active_tables
        ranked = sorted(
            non_hier,
            key=lambda t: (
                1
                if any(
                    _is_numeric_measure_column(
                        c, t, join_keys=join_keys, pk_cols=set(_effective_pk_columns(t))
                    )
                    for c in t.columns
                )
                else 0,
                t.row_count or 0,
                len(t.columns),
            ),
            reverse=True,
        )
        anchors = ranked[:3]

    return anchors


def _databricks_measure_format(col: ColumnInfo) -> dict[str, Any] | None:
    """Format hint for Databricks Metric Views v1.1 (check percentage BEFORE currency)."""
    name_to_check = col.semantic_name or col.name
    if _PERCENT_MEASURE_RE.search(name_to_check) or _PERCENT_MEASURE_RE.search(col.name):
        return {"type": "percentage", "decimal_places": {"type": "exact", "places": 1}}
    if _CURRENCY_MEASURE_RE.search(name_to_check) or _CURRENCY_MEASURE_RE.search(col.name):
        return {"type": "currency", "currency_code": "USD", "decimal_places": {"type": "exact", "places": 2}}
    if col.canonical_type in {"decimal", "float"}:
        return {"type": "number", "decimal_places": {"type": "exact", "places": 2}}
    return None


def _collect_safe_metric_view_joins(
    tbl: TableInfo,
    cat: str,
    silver_schema: str,
    active_by_full: dict[str, TableInfo],
    verified_rels: list[RelationshipInfo],
    dbx_tbl_map: dict[str, str],
    dbx_col_maps: dict[str, dict[str, str]],
    logical_tbl_map: dict[str, str],
) -> tuple[list[dict[str, Any]], list[tuple[str, TableInfo]]]:
    """Build fan-out-safe many-to-one joins (supports compound explicit FKs; enforces target uniqueness)."""
    joins_yaml: list[dict[str, Any]] = []
    joined_tables: list[tuple[str, TableInfo]] = []
    seen_join_aliases: set[str] = {"source"}
    seen_join_targets: set[str] = set()
    col_map = dbx_col_maps[tbl.full_name]

    def _alloc_alias(tgt_tbl: TableInfo) -> str:
        base_alias = _slug(logical_tbl_map[tgt_tbl.full_name])
        alias = base_alias
        sfx = 2
        while alias in seen_join_aliases:
            alias = f"{base_alias}_{sfx}"
            sfx += 1
        seen_join_aliases.add(alias)
        return alias

    # 1. Explicit FKs first — preserves compound multi-column FKs as AND predicates!
    for fk in tbl.foreign_keys:
        if len(joins_yaml) >= 6:
            break
        if len(fk.constrained_columns) != len(fk.referred_columns) or not fk.constrained_columns:
            continue
        tgt_tbl = _resolve_referred_table(tbl, fk.referred_schema, fk.referred_table, active_by_full)
        if tgt_tbl is None or tgt_tbl.full_name == tbl.full_name or tgt_tbl.full_name in seen_join_targets:
            continue
        if not _is_unique_column_set(tgt_tbl, fk.referred_columns):
            continue
        tgt_col_map = dbx_col_maps[tgt_tbl.full_name]
        pairs: list[tuple[str, str]] = []
        valid = True
        for sc, tc in zip(fk.constrained_columns, fk.referred_columns):
            sc_dbx = _resolve_col_name_ci(col_map, sc)
            tc_dbx = _resolve_col_name_ci(tgt_col_map, tc)
            if not sc_dbx or not tc_dbx:
                valid = False
                break
            pairs.append((sc_dbx, tc_dbx))
        if not valid:
            continue
        alias = _alloc_alias(tgt_tbl)
        seen_join_targets.add(tgt_tbl.full_name)
        tgt_safe = dbx_tbl_map[tgt_tbl.full_name]
        on_clause = " AND ".join(
            f"source.{_spark_ident(sc_dbx)} = {_spark_ident(alias)}.{_spark_ident(tc_dbx)}"
            for sc_dbx, tc_dbx in pairs
        )
        joins_yaml.append(
            {
                "name": alias,
                "source": _spark_table_ref(cat, silver_schema, f"silver_{tgt_safe}"),
                "on": on_clause,
            }
        )
        joined_tables.append((alias, tgt_tbl))

    # 2. Single-column verified relationships (strictly requiring unique target column -> zero fan-out!)
    outgoing = [r for r in verified_rels if r.source_table == tbl.full_name]
    for rel in outgoing:
        if len(joins_yaml) >= 6:
            break
        if rel.target_table in seen_join_targets:
            continue
        tgt_tbl = active_by_full[rel.target_table]
        if not _is_unique_single_key(tgt_tbl, rel.target_column):
            continue
        tgt_col_map = dbx_col_maps[tgt_tbl.full_name]
        src_col_dbx = _resolve_col_name_ci(col_map, rel.source_column)
        tgt_col_dbx = _resolve_col_name_ci(tgt_col_map, rel.target_column)
        if not src_col_dbx or not tgt_col_dbx:
            continue

        alias = _alloc_alias(tgt_tbl)
        seen_join_targets.add(rel.target_table)
        tgt_safe = dbx_tbl_map[tgt_tbl.full_name]
        joins_yaml.append(
            {
                "name": alias,
                "source": _spark_table_ref(cat, silver_schema, f"silver_{tgt_safe}"),
                "on": f"source.{_spark_ident(src_col_dbx)} = {_spark_ident(alias)}.{_spark_ident(tgt_col_dbx)}",
            }
        )
        joined_tables.append((alias, tgt_tbl))

    return joins_yaml, joined_tables


def _build_databricks_metric_view_specs(
    result: DiscoveryResult,
    catalog: str | None = None,
    silver_schema: str = "silver",
    gold_schema: str = "gold",
) -> list[dict[str, Any]]:
    """Single source of truth for Databricks Metric Views (used by both metric_views.sql & genie_context.md)."""
    cat = catalog or result.config.databricks_catalog
    all_base_tables = [t for t in result.tables if t.table_type == "table"]
    dbx_tbl_map = deduplicate_table_names(all_base_tables)
    active_tables, _ = _select_active_base_tables(result)
    active_by_full = {t.full_name: t for t in active_tables}
    logical_tbl_map = _deduplicate_logical_table_names(active_tables)
    verified_rels = _filtered_verified_relationships(result, active_by_full)
    join_keys = _collect_join_key_columns(result)

    anchors = _choose_metric_view_anchors(active_tables, verified_rels, join_keys)
    hier_tables = [t for t in get_hierarchy_tables(result) if t.full_name in active_by_full]

    dbx_col_maps: dict[str, dict[str, str]] = {
        t.full_name: deduplicate_column_names([c.name for c in t.columns]) for t in active_tables
    }
    legacy_dbx_maps: dict[str, dict[str, tuple[str, str, bool]]] = {
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

    specs: list[dict[str, Any]] = []

    for tbl in anchors:
        safe_tbl = dbx_tbl_map[tbl.full_name]
        logical_tname = logical_tbl_map[tbl.full_name]
        view_name = _alloc_view_name(logical_tname)
        view_fqn = _spark_table_ref(cat, gold_schema, view_name)
        src_fqn = _spark_table_ref(cat, silver_schema, f"silver_{safe_tbl}")
        col_map = dbx_col_maps[tbl.full_name]
        legacy_map = legacy_dbx_maps[tbl.full_name]
        eff_pk_set = set(_effective_pk_columns(tbl))

        joins_yaml, joined_tables = _collect_safe_metric_view_joins(
            tbl=tbl,
            cat=cat,
            silver_schema=silver_schema,
            active_by_full=active_by_full,
            verified_rels=verified_rels,
            dbx_tbl_map=dbx_tbl_map,
            dbx_col_maps=dbx_col_maps,
            logical_tbl_map=logical_tbl_map,
        )

        dimensions_yaml: list[dict[str, Any]] = []
        measures_yaml: list[dict[str, Any]] = []
        seen_dim_names: set[str] = set()
        seen_meas_names: set[str] = set()
        preferred_string_dim: str | None = None

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
        primary_time_is_ts: bool = False
        ot_ts_col_name = tbl.telemetry_profile.timestamp_column if tbl.telemetry_profile else None
        measure_cols: list[tuple[ColumnInfo, str]] = []

        for col in tbl.columns:
            if col.canonical_type == "binary":
                continue
            is_join_or_pk = (
                col.is_primary_key
                or col.name in eff_pk_set
                or (tbl.full_name, col.name.lower()) in join_keys
            )
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

            # Legacy parsed date/timestamp (or unpaired HHMMSS string)
            if col.name in legacy_map:
                derived_col, derived_type, is_temporal = legacy_map[col.name]
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
                if is_temporal and (primary_time_expr is None or col.name == ot_ts_col_name):
                    primary_time_expr = expr
                    primary_time_slug = dname
                    primary_time_is_ts = derived_type == "TIMESTAMP"
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
                is_col_ts = col.canonical_type == "timestamp"
                if (
                    col.name == ot_ts_col_name
                    or primary_time_expr is None
                    or (tbl.telemetry_profile and is_col_ts and not primary_time_is_ts)
                ):
                    primary_time_expr = expr
                    primary_time_slug = dname
                    primary_time_is_ts = is_col_ts
                continue

            # Quantitative numeric measures
            if _is_numeric_measure_column(col, tbl, join_keys=join_keys, pk_cols=eff_pk_set):
                measure_cols.append((col, dbx_col))
                continue

            # Regular dimension (capped at 20 source dimensions for readability)
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
                if (
                    preferred_string_dim is None
                    and col.canonical_type in {"string", "boolean"}
                    and not col.is_primary_key
                    and not _is_direct_pii(col)
                    and not _is_itar_or_ip(col)
                ):
                    preferred_string_dim = dname

        # Calendar / hourly grain helper dimension for primary time column
        if primary_time_expr and primary_time_slug:
            if tbl.telemetry_profile and primary_time_is_ts:
                grain_unit, grain_suffix = "HOUR", "hour"
            elif tbl.telemetry_profile:
                grain_unit, grain_suffix = "DAY", "day"
            else:
                grain_unit, grain_suffix = "MONTH", "month"
            gname = _alloc_dim(f"{primary_time_slug}_{grain_suffix}")
            dimensions_yaml.append(
                {
                    "name": gname,
                    "expr": f"DATE_TRUNC('{grain_unit}', {primary_time_expr})",
                    "display_name": f"{_human_label(primary_time_slug)} ({grain_suffix.title()})",
                    "comment": f"{grain_suffix.title()} time bucket derived from {primary_time_slug}.",
                }
            )

        # Top non-PII descriptive dimensions from joined dimension tables (up to 4 per join)
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

        first_numeric_measure: str | None = None
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
            if first_numeric_measure is None:
                first_numeric_measure = mname

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

        specs.append(
            {
                "view_name": view_name,
                "view_fqn": view_fqn,
                "tbl": tbl,
                "logical_name": logical_tname,
                "is_hierarchy": False,
                "yaml_spec": mv_yaml,
                "benchmark_dim": preferred_string_dim or (dimensions_yaml[0]["name"] if dimensions_yaml else None),
                "benchmark_measure": first_numeric_measure or cnt_name,
            }
        )

    # Gold Recursive Hierarchy / BOM Metric Views
    for htbl in hier_tables:
        assert htbl.hierarchy is not None
        safe_h = dbx_tbl_map[htbl.full_name]
        logical_h = logical_tbl_map.get(htbl.full_name, _slug(htbl.name))
        is_bom = htbl.hierarchy.pattern == "bom"
        h_suffix = "bom" if is_bom else "hierarchy"
        h_label = "BOM" if is_bom else "Hierarchy"

        view_name = _alloc_view_name(f"{logical_h}_{h_suffix}")
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
                {"name": "root_node_id", "expr": "source.root_node_id", "display_name": "Root Assembly" if is_bom else "Root Node"},
                {"name": "parent_node_id", "expr": "source.parent_node_id", "display_name": "Parent Component" if is_bom else "Parent Node"},
                {"name": "child_node_id", "expr": "source.child_node_id", "display_name": "Child Component" if is_bom else "Child Node"},
                {"name": "depth", "expr": "source.depth", "display_name": f"{h_label} Depth"},
            ]
        )
        max_depth_meas = "max_bom_depth" if is_bom else "max_hierarchy_depth"
        h_measures: list[dict[str, Any]] = [
            {
                "name": "exploded_component_links",
                "expr": "COUNT(1)",
                "display_name": "Exploded Component Links" if is_bom else "Exploded Hierarchy Links",
                "comment": f"Total exploded parent-child {h_label.lower()} edges.",
            },
            {
                "name": max_depth_meas,
                "expr": "MAX(source.depth)",
                "display_name": f"Max {h_label} Depth",
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
        specs.append(
            {
                "view_name": view_name,
                "view_fqn": view_fqn,
                "tbl": htbl,
                "logical_name": logical_h,
                "is_hierarchy": True,
                "yaml_spec": hmv_yaml,
                "benchmark_dim": "root_node_id",
                "benchmark_measure": "total_extended_qty" if qty_obj else "exploded_component_links",
                "max_depth_measure": max_depth_meas,
            }
        )

    return specs


def render_databricks_metric_views_sql(
    result: DiscoveryResult,
    catalog: str | None = None,
    silver_schema: str = "silver",
    gold_schema: str = "gold",
) -> str:
    """Generate Databricks Unity Catalog Metric Views SQL (WITH METRICS LANGUAGE YAML, spec v1.1)."""
    cat = catalog or result.config.databricks_catalog
    specs = _build_databricks_metric_view_specs(
        result,
        catalog=cat,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
    )

    lines: list[str] = [
        f"-- Auto-generated by tick: Databricks Unity Catalog Metric Views ({cat}.{gold_schema})",
        f"-- Source database: {result.overview.database_name} ({result.overview.dialect})",
        "-- Requires Databricks Runtime 17.3+ / Serverless SQL Warehouse with Unity Catalog Metric Views (v1.1).",
        "",
        f"CREATE SCHEMA IF NOT EXISTS {_spark_ident(cat)}.{_spark_ident(gold_schema)};",
        "",
    ]

    if not specs:
        lines.append("-- No eligible base tables discovered to generate Unity Catalog Metric Views.")
        return "\n".join(lines) + "\n"

    for spec in specs:
        tbl: TableInfo = spec["tbl"]
        view_fqn: str = spec["view_fqn"]
        mv_yaml: dict[str, Any] = spec["yaml_spec"]
        yaml_body = yaml.safe_dump(mv_yaml, sort_keys=False, width=110).rstrip()
        dq_tag = _pick_dollar_quote_tag(yaml_body)

        if spec["is_hierarchy"] and tbl.hierarchy is not None:
            comment_hdr = f"-- Gold Recursive Hierarchy Metric View for {tbl.full_name} ({tbl.hierarchy.pattern})"
            ex_sql = (
                f"-- Example query: SELECT root_node_id, MEASURE(exploded_component_links), "
                f"MEASURE({spec['max_depth_measure']}) FROM {view_fqn} GROUP BY 1 ORDER BY 2 DESC LIMIT 20;"
            )
        else:
            comment_hdr = f"-- Metric View for {tbl.full_name}" + (f" ({tbl.semantic_name})" if tbl.semantic_name else "")
            sample_dim = spec["benchmark_dim"]
            sample_meas = spec["benchmark_measure"]
            ex_sql = (
                f"-- Example query: SELECT {_spark_ident(sample_dim)}, MEASURE({_spark_ident(sample_meas)}) "
                f"FROM {view_fqn} GROUP BY 1 ORDER BY 2 DESC LIMIT 20;"
                if sample_dim
                else ""
            )

        lines.extend(
            [
                comment_hdr,
                f"CREATE OR REPLACE VIEW {view_fqn}",
                "WITH METRICS",
                "LANGUAGE YAML",
                f"AS {dq_tag}",
                yaml_body,
                f"{dq_tag};",
            ]
        )
        if ex_sql:
            lines.append(ex_sql)
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


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
    verified_rels = _filtered_verified_relationships(result, active_by_full)
    hier_tables = [t for t in get_hierarchy_tables(result) if t.full_name in active_by_full]

    mv_specs = _build_databricks_metric_view_specs(
        result,
        catalog=cat,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
    )

    dbx_col_maps: dict[str, dict[str, str]] = {
        t.full_name: deduplicate_column_names([c.name for c in t.columns]) for t in active_tables
    }
    legacy_dbx_maps: dict[str, dict[str, tuple[str, str, bool]]] = {
        t.full_name: _compute_databricks_silver_legacy_cols(t, dbx_col_maps[t.full_name])
        for t in active_tables
    }

    lines: list[str] = [
        f"# Databricks AI/BI Genie Space Blueprint: {_md_code(result.overview.database_name)}",
        "",
        f"- **Source System:** `{result.overview.dialect}` ({_escape_md(result.overview.database_name)})",
        f"- **Target Unity Catalog:** {_md_code(cat)} (Bronze: {_md_code(bsch)} · Silver: {_md_code(silver_schema)} · Gold: {_md_code(gold_schema)})",
        f"- **Active Data Assets:** `{len(active_tables)}` Silver tables · `{len(mv_specs)}` Metric Views · `{len(verified_rels)}` verified joins",
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
        if any(is_tmp for _, _, is_tmp in legacy_dbx_maps[tbl.full_name].values()):
            notes.append("Legacy dates parsed")
        if tbl.hierarchy:
            notes.append(f"Use Gold exploded view for multi-level {tbl.hierarchy.pattern}")
        lines.append(
            f"| Silver Table | {_md_code(fqn)} | {_md_code(tbl.semantic_name or tbl.name)} | "
            f"`{role}` | {_escape_md('; '.join(notes) or (tbl.business_description or tbl.comment))} |"
        )

    for htbl in hier_tables:
        safe_h = dbx_tbl_map[htbl.full_name]
        lines.append(
            f"| Gold Hierarchy | {_md_code(f'{cat}.{gold_schema}.gold_{safe_h}_exploded')} | "
            f"{_md_code(htbl.semantic_name or htbl.name)} exploded | `hierarchy` | "
            f"Pre-exploded recursive {htbl.hierarchy.pattern if htbl.hierarchy else 'tree'} |"
        )

    for spec in mv_specs:
        mv_name = spec["view_name"]
        logical_name = spec["logical_name"]
        kind_desc = (
            "Governed recursive rollup `MEASURE(...)` definitions (`metric_views.sql`)"
            if spec["is_hierarchy"]
            else "Governed `MEASURE(...)` definitions (`metric_views.sql`)"
        )
        lines.append(
            f"| Gold Metric View | {_md_code(f'{cat}.{gold_schema}.{mv_name}')} | "
            f"{_md_code(logical_name)} KPIs | `metric_view` | {kind_desc} |"
        )

    if omitted_empty:
        lines.append("")
        lines.append(
            f"> **Excluded Empty Tables ({len(omitted_empty)}):** "
            + ", ".join(_md_code(t.full_name) for t in omitted_empty[:15])
            + (" ..." if len(omitted_empty) > 15 else "")
        )

    # 2. Copy-Paste Genie General Instructions
    instr_lines: list[str] = [
        f"You are an analytics assistant for {result.overview.database_name} data stored in Unity Catalog `{cat}`.",
        f"1. ALWAYS query cleaned Silver tables in `{cat}.{silver_schema}` or Gold Metric Views in `{cat}.{gold_schema}` — NEVER query raw Bronze tables in `{cat}.{bsch}`.",
        f"2. When answering KPI or aggregation questions, prefer Unity Catalog Metric Views (`{cat}.{gold_schema}.mv_*`) using MEASURE(<metric_name>) so calculations remain consistent across dashboards.",
    ]

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
        instr_lines.append(f"{step_no}. ERP/MES Business Glossary mappings: {terms}.")
        step_no += 1

    legacy_notes = [
        f"silver_{dbx_tbl_map[t.full_name]}.{derived_col} (from {raw_col})"
        for t in active_tables
        for raw_col, (derived_col, _, is_tmp) in legacy_dbx_maps[t.full_name].items()
        if is_tmp
    ]
    if legacy_notes:
        instr_lines.append(
            f"{step_no}. Legacy date/time fields are pre-parsed in Silver — use {', '.join(legacy_notes[:8])} instead of raw integer/Julian codes."
        )
        step_no += 1

    ot_tables = [t for t in active_tables if t.telemetry_profile is not None]
    if ot_tables:
        ot_list = ", ".join(f"silver_{dbx_tbl_map[t.full_name]}" for t in ot_tables)
        instr_lines.append(
            f"{step_no}. OT Telemetry ({ot_list}): Silver Lakeflow expectations flag/filter bad OPC sensor quality. Bucket time-series queries with DATE_TRUNC('hour', ...) or DATE_TRUNC('day', ...)."
        )
        step_no += 1

    if hier_tables:
        h_list = ", ".join(f"`{cat}.{gold_schema}.gold_{dbx_tbl_map[h.full_name]}_exploded`" for h in hier_tables)
        has_any_qty = any(_find_quantity_column(h) is not None for h in hier_tables)
        h_cols_hint = (
            "`root_node_id`, `child_node_id`, `depth`, `extended_qty`"
            if has_any_qty
            else "`root_node_id`, `child_node_id`, `depth`, `hierarchy_path`"
        )
        instr_lines.append(
            f"{step_no}. Multi-level BOM / hierarchy queries: ALWAYS use {h_list} ({h_cols_hint}) rather than writing recursive CTEs ad hoc."
        )
        step_no += 1

    pii_cols_list = [
        f"silver_{dbx_tbl_map[t.full_name]}.{dbx_col_maps[t.full_name][c.name]}"
        for t in active_tables
        for c in t.columns
        if _is_direct_pii(c) or _is_itar_or_ip(c)
    ]
    if pii_cols_list:
        instr_lines.append(
            f"{step_no}. Governance & Privacy: NEVER display raw PII or export-controlled IP columns ({', '.join(pii_cols_list[:10])}); rely on Unity Catalog column masks and aggregate results."
        )
        step_no += 1

    plant_cols_list = [
        f"silver_{dbx_tbl_map[t.full_name]}.{dbx_col_maps[t.full_name][c.name]}"
        for t in active_tables
        for c in t.columns
        if _is_plant_key(c)
    ]
    if plant_cols_list:
        instr_lines.append(
            f"{step_no}. Multi-Plant Scoping: Include or filter by plant isolation keys ({', '.join(plant_cols_list[:6])}) when comparing facilities."
        )

    instr_body = "\n".join(instr_lines)
    instr_fence = _md_fence(instr_body)
    lines.extend(
        [
            "",
            "---",
            "",
            "## 2. Copy-Paste Genie General Instructions",
            "",
            "Paste the block below into **Genie Space → Configure → Instructions → Text**:",
            "",
            f"{instr_fence}text",
            instr_body,
            instr_fence,
        ]
    )

    # 3. Verified Join Definitions for Genie Space
    lines.extend(["", "---", "", "## 3. Verified Join Paths (for Genie Space Joins)", ""])
    if verified_rels:
        lines.append("| Left Silver Table | Right Silver Table | Join Condition (`ON`) | Kind | Confidence |")
        lines.append("|---|---|---|---|---:|")
        for rel in verified_rels:
            l_safe = dbx_tbl_map[rel.source_table]
            r_safe = dbx_tbl_map[rel.target_table]
            l_col = _resolve_col_name_ci(dbx_col_maps[rel.source_table], rel.source_column) or sanitize_delta_column_name(rel.source_column)
            r_col = _resolve_col_name_ci(dbx_col_maps[rel.target_table], rel.target_column) or sanitize_delta_column_name(rel.target_column)
            on_expr = f"silver_{l_safe}.{_spark_ident(l_col)} = silver_{r_safe}.{_spark_ident(r_col)}"
            lines.append(
                f"| {_md_code(f'silver_{l_safe}')} | {_md_code(f'silver_{r_safe}')} | {_md_code(on_expr)} | "
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
                val_str = ", ".join(_md_code(v) for v in vals)
                enum_tag = "Exact enum" if is_enum else "Top values"
                filter_rows.append(
                    f"| {_md_code(f'silver_{safe_tbl}')} | {_md_code(dbx_c)} | "
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
        mv_specs=mv_specs,
        hier_tables=hier_tables,
        dbx_tbl_map=dbx_tbl_map,
        dbx_col_maps=dbx_col_maps,
        verified_rels=verified_rels,
    )
    for idx, (question, sql_text) in enumerate(benchmarks, start=1):
        sql_fence = _md_fence(sql_text)
        lines.extend(
            [
                f"### Q{idx}: {question}",
                "",
                f"{sql_fence}sql",
                sql_text,
                sql_fence,
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _build_genie_benchmark_queries(
    cat: str,
    silver_schema: str,
    gold_schema: str,
    active_tables: list[TableInfo],
    mv_specs: list[dict[str, Any]],
    hier_tables: list[TableInfo],
    dbx_tbl_map: dict[str, str],
    dbx_col_maps: dict[str, dict[str, str]],
    verified_rels: list[RelationshipInfo],
) -> list[tuple[str, str]]:
    queries: list[tuple[str, str]] = []

    # Q1: Grounded directly in the exact emitted first non-hierarchy Metric View spec (zero drift!)
    first_mv = next((s for s in mv_specs if not s["is_hierarchy"]), mv_specs[0] if mv_specs else None)
    if first_mv:
        mv_fqn = first_mv["view_fqn"]
        logical_name = first_mv["logical_name"]
        b_dim = first_mv["benchmark_dim"]
        b_meas = first_mv["benchmark_measure"]
        m_expr = f"MEASURE({_spark_ident(b_meas)})"
        if b_dim:
            queries.append(
                (
                    f"What is the breakdown of {_human_label(logical_name).lower()} by {_human_label(b_dim).lower()}?",
                    f"SELECT {_spark_ident(b_dim)}, {m_expr} AS metric_value\n"
                    f"FROM {mv_fqn}\n"
                    f"GROUP BY 1\n"
                    f"ORDER BY 2 DESC\n"
                    f"LIMIT 20;",
                )
            )
        else:
            queries.append(
                (
                    f"What is the overall {_human_label(logical_name).lower()} summary?",
                    f"SELECT {m_expr} AS metric_value\nFROM {mv_fqn};",
                )
            )

    # Q2: Joined Silver query if a verified relationship exists (case-insensitive safe column lookup)
    if verified_rels:
        rel = verified_rels[0]
        l_safe = dbx_tbl_map[rel.source_table]
        r_safe = dbx_tbl_map[rel.target_table]
        l_fqn = _spark_table_ref(cat, silver_schema, f"silver_{l_safe}")
        r_fqn = _spark_table_ref(cat, silver_schema, f"silver_{r_safe}")
        l_col = _resolve_col_name_ci(dbx_col_maps[rel.source_table], rel.source_column) or sanitize_delta_column_name(rel.source_column)
        r_col = _resolve_col_name_ci(dbx_col_maps[rel.target_table], rel.target_column) or sanitize_delta_column_name(rel.target_column)
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

    # Q3: OT Telemetry benchmark query
    ot_tbl = next((t for t in active_tables if t.telemetry_profile is not None), None)
    if ot_tbl and ot_tbl.telemetry_profile:
        safe_ot = dbx_tbl_map[ot_tbl.full_name]
        ot_fqn = _spark_table_ref(cat, silver_schema, f"silver_{safe_ot}")
        ts_raw = ot_tbl.telemetry_profile.timestamp_column
        ts_obj = next((c for c in ot_tbl.columns if c.name == ts_raw), None)
        ts_c = _resolve_col_name_ci(dbx_col_maps[ot_tbl.full_name], ts_raw) or sanitize_delta_column_name(ts_raw)
        is_ts = ts_obj is None or ts_obj.canonical_type == "timestamp"
        trunc_unit = "HOUR" if is_ts else "DAY"
        bucket_lbl = "hourly" if is_ts else "daily"
        bucket_col = "reading_hour" if is_ts else "reading_day"
        val_raw = ot_tbl.telemetry_profile.value_columns[0] if ot_tbl.telemetry_profile.value_columns else None
        val_c = _resolve_col_name_ci(dbx_col_maps[ot_tbl.full_name], val_raw) if val_raw else None
        avg_part = f", AVG({_spark_ident(val_c)}) AS avg_{val_c}" if val_c else ""
        queries.append(
            (
                f"Show {bucket_lbl} sensor reading volume for {_human_label(safe_ot).lower()}.",
                f"SELECT DATE_TRUNC('{trunc_unit}', {_spark_ident(ts_c)}) AS {bucket_col},\n"
                f"       COUNT(*) AS reading_count{avg_part}\n"
                f"FROM {ot_fqn}\n"
                f"GROUP BY 1\n"
                f"ORDER BY 1 DESC\n"
                f"LIMIT 48;",
            )
        )

    # Q4: Gold Hierarchy / BOM benchmark query
    if hier_tables:
        ht = hier_tables[0]
        safe_h = dbx_tbl_map[ht.full_name]
        gold_fqn = _spark_table_ref(cat, gold_schema, f"gold_{safe_h}_exploded")
        has_qty = _find_quantity_column(ht) is not None
        qty_part = ", SUM(extended_qty) AS total_extended_qty" if has_qty else ""
        h_word = "BOM" if (ht.hierarchy and ht.hierarchy.pattern == "bom") else "hierarchy"
        queries.append(
            (
                f"Which root nodes have the deepest and largest exploded {h_word} in {_human_label(safe_h).lower()}?",
                f"SELECT root_node_id, COUNT(*) AS component_links, MAX(depth) AS max_depth{qty_part}\n"
                f"FROM {gold_fqn}\n"
                f"GROUP BY 1\n"
                f"ORDER BY component_links DESC\n"
                f"LIMIT 25;",
            )
        )

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
