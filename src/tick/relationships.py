"""Explicit FK cataloging, automatic join/relationship inference, and view SQL lineage."""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

from tick.dialects import DialectAdapter
from tick.models import (
    CanonicalType,
    ColumnInfo,
    DiscoveryConfig,
    LineageEdge,
    RelationshipInfo,
    TableInfo,
)

DW_PREFIXES = ("dim_", "fct_", "fact_", "stg_", "raw_", "src_", "tbl_", "t_", "ref_", "lkp_", "v_", "vw_")


def _strip_dw_prefix(name: str) -> str:
    lower = name.lower()
    for prefix in DW_PREFIXES:
        if lower.startswith(prefix) and len(lower) > len(prefix):
            return lower[len(prefix) :]
    return lower


def _singularize(word: str) -> str:
    w = word.lower()
    if w.endswith("ies") and len(w) > 4:
        return w[:-3] + "y"
    if w.endswith(("ses", "xes", "zes", "ches", "shes")) and len(w) > 4:
        return w[:-2]
    if w.endswith("s") and not w.endswith(("ss", "us", "is")) and len(w) > 2:
        return w[:-1]
    return w


def _table_stem_variants(table_name: str) -> set[str]:
    base = table_name.lower()
    stripped = _strip_dw_prefix(base)
    variants = {base, stripped, _singularize(base), _singularize(stripped)}
    # Also include last token of compound names like ecom_customers -> customer
    if "_" in stripped:
        last = stripped.rsplit("_", 1)[-1]
        variants.add(last)
        variants.add(_singularize(last))
    return {v for v in variants if len(v) >= 2}


def _compatible_types(a: CanonicalType, b: CanonicalType) -> bool:
    if a == b:
        return True
    if {a, b} <= {"uuid", "string"}:
        return True
    if {a, b} <= {"integer", "decimal"}:
        return True
    return False


def _is_candidate_target_key(col: ColumnInfo, table: TableInfo) -> bool:
    if col.is_primary_key and len(table.primary_key) == 1:
        return True
    if col.is_unique:
        return True
    if col.profile and col.profile.cardinality_class == "unique" and col.profile.null_rate == 0.0:
        if col.canonical_type in {"integer", "string", "uuid", "decimal"}:
            name_l = col.name.lower()
            if name_l == "id" or name_l.endswith(("_id", "_key", "_uuid", "_code", "_sku")):
                return True
    return False


def _match_column_to_target(
    src_col_name: str, target_table: TableInfo, target_col: ColumnInfo
) -> tuple[float, str] | None:
    """Return (base_name_score, reason) if src_col_name plausibly references target_table.target_col."""
    src = src_col_name.lower()
    tgt = target_col.name.lower()
    stems = _table_stem_variants(target_table.name)

    # Pattern 1: exact same key column name (e.g., customer_id -> dim_customers.customer_id)
    if src == tgt and (src.endswith(("_id", "_key", "_uuid", "_code", "_sku"))):
        if any(stem in src for stem in stems):
            return 0.86, f"Exact key column name '{src}' matches target '{target_table.name}.{tgt}'"
        # Shared non-generic key name like account_uuid
        if src not in {"id", "key", "uuid", "code", "parent_id"}:
            return 0.72, f"Shared key column name '{src}' with unique target '{target_table.name}.{tgt}'"

    # Pattern 2: <stem>_id -> <table>.id  (or <stem>_uuid -> <table>.uuid, <stem>_key -> <table>.key)
    # Sort longest stems first and keep highest-scoring hit so PYTHONHASHSEED never alters results
    best_match: tuple[float, str] | None = None
    for stem in sorted(stems, key=lambda s: (-len(s), s)):
        cand_match: tuple[float, str] | None = None
        if src == f"{stem}_{tgt}":
            cand_match = (0.88, f"Column '{src}' matches '{target_table.name}' stem + '.{tgt}'")
        elif tgt in {"id", f"{stem}_id"} and src == f"{stem}_id":
            cand_match = (0.88, f"Column '{src}' matches standard FK convention to '{target_table.name}.{tgt}'")
        elif tgt == "id" and src.endswith(f"_{stem}_id"):
            cand_match = (0.76, f"Role-prefixed FK column '{src}' ends with '_{stem}_id' -> '{target_table.name}.id'")
        elif tgt in {"uuid", "guid"} and src == f"{stem}_{tgt}":
            cand_match = (0.84, f"UUID FK column '{src}' matches '{target_table.name}.{tgt}'")
        if cand_match is not None and (best_match is None or cand_match[0] > best_match[0]):
            best_match = cand_match

    return best_match


def discover_relationships(
    tables: list[TableInfo],
    value_samples: dict[tuple[str, str], set[str]],
    config: DiscoveryConfig,
    value_sample_complete: dict[tuple[str, str], bool] | None = None,
) -> tuple[list[RelationshipInfo], list[LineageEdge]]:
    """Collect explicit FKs and infer missing/unenforced relationships between tables."""
    relationships: list[RelationshipInfo] = []
    lineage_edges: list[LineageEdge] = []

    table_by_full = {t.full_name: t for t in tables}
    table_by_full_lower = {t.full_name.lower(): t.full_name for t in tables}
    table_by_name: dict[str, list[TableInfo]] = {}
    for t in tables:
        table_by_name.setdefault(t.name.lower(), []).append(t)

    explicit_pairs: set[tuple[str, str, str, str]] = set()

    # 1. Explicit Foreign Keys
    for tbl in tables:
        for fk in tbl.foreign_keys:
            ref_schema = fk.referred_schema or tbl.schema_name
            target_full = f"{ref_schema}.{fk.referred_table}"
            if target_full not in table_by_full:
                ci_match = table_by_full_lower.get(target_full.lower())
                if ci_match:
                    target_full = ci_match
                elif not fk.referred_schema:
                    candidates = table_by_name.get(fk.referred_table.lower(), [])
                    if len(candidates) == 1:
                        target_full = candidates[0].full_name

            for src_col, tgt_col in zip(fk.constrained_columns, fk.referred_columns):
                pair = (tbl.full_name, src_col, target_full, tgt_col)
                if pair in explicit_pairs:
                    continue
                explicit_pairs.add(pair)
                relationships.append(
                    RelationshipInfo(
                        source_table=tbl.full_name,
                        source_column=src_col,
                        target_table=target_full,
                        target_column=tgt_col,
                        kind="explicit_fk",
                        confidence=1.0,
                        overlap_rate=None,
                        evidence=[f"Declared foreign key constraint{f' ({fk.name})' if fk.name else ''}"],
                    )
                )
                lineage_edges.append(
                    LineageEdge(
                        upstream_table=target_full,
                        downstream_table=tbl.full_name,
                        edge_type="foreign_key",
                        details=f"{tbl.name}.{src_col} -> {fk.referred_table}.{tgt_col}",
                    )
                )

    if not config.infer_relationships:
        return relationships, lineage_edges

    # 2. Candidate Target Keys (base tables only)
    target_keys: list[tuple[TableInfo, ColumnInfo]] = []
    for tbl in tables:
        if tbl.table_type != "table":
            continue
        for col in tbl.columns:
            if _is_candidate_target_key(col, tbl):
                target_keys.append((tbl, col))

    # 3. Infer missing relationships
    for src_tbl in tables:
        if src_tbl.table_type != "table":
            continue
        for src_col in src_tbl.columns:
            # Skip single-column primary keys (usually entities, not outbound FKs)
            if src_col.is_primary_key and len(src_tbl.primary_key) == 1:
                continue
            if src_col.canonical_type not in {"integer", "string", "uuid", "decimal"}:
                continue

            best_candidate: RelationshipInfo | None = None
            best_same_schema = False

            for tgt_tbl, tgt_col in target_keys:
                if tgt_tbl.full_name == src_tbl.full_name:
                    # Allow self-referential parent_id only if explicitly named parent_id -> id
                    if not (src_col.name.lower() == "parent_id" and tgt_col.name.lower() == "id"):
                        continue
                if not _compatible_types(src_col.canonical_type, tgt_col.canonical_type):
                    continue
                pair = (src_tbl.full_name, src_col.name, tgt_tbl.full_name, tgt_col.name)
                if pair in explicit_pairs:
                    continue

                if src_col.name.lower() == "parent_id" and tgt_tbl.full_name == src_tbl.full_name and tgt_col.name.lower() == "id":
                    match = (0.78, "Self-referential 'parent_id' matches table primary key 'id'")
                else:
                    match = _match_column_to_target(src_col.name, tgt_tbl, tgt_col)

                if not match:
                    continue

                base_score, reason = match
                evidence = [reason]
                overlap_rate: float | None = None
                kind: str = "inferred_name"
                final_score = base_score

                # Bonus if target is an explicit PK
                if tgt_col.is_primary_key:
                    final_score = min(0.95, final_score + 0.04)
                    evidence.append(f"Target '{tgt_tbl.name}.{tgt_col.name}' is a primary key")

                # Value overlap verification using profiled samples
                if config.verify_relationship_values:
                    src_vals = value_samples.get((src_tbl.full_name, src_col.name))
                    tgt_vals = value_samples.get((tgt_tbl.full_name, tgt_col.name))
                    if src_vals and tgt_vals:
                        if {src_col.canonical_type, tgt_col.canonical_type} == {"integer", "decimal"}:
                            src_cmp = {re.sub(r"\.0+$", "", v.strip()) for v in src_vals}
                            tgt_cmp = {re.sub(r"\.0+$", "", v.strip()) for v in tgt_vals}
                        else:
                            src_cmp = src_vals
                            tgt_cmp = tgt_vals
                        overlap = len(src_cmp & tgt_cmp) / len(src_cmp)
                        overlap_rate = round(overlap, 4)
                        if overlap >= 0.50:
                            kind = "inferred_name_and_values"
                            final_score = min(0.98, 0.70 + 0.28 * overlap)
                            evidence.append(
                                f"{overlap:.0%} of sampled '{src_col.name}' values exist in '{tgt_tbl.name}.{tgt_col.name}'"
                            )
                        elif overlap > 0.0:
                            kind = "inferred_name_and_values"
                            final_score = round(base_score * 0.75, 3)
                            evidence.append(
                                f"Partial value overlap ({overlap:.0%}) between sampled values"
                            )
                        else:
                            # Only reject on 0% overlap if target sample truly covers the entire target table
                            if value_sample_complete is not None:
                                tgt_fully_sampled = bool(
                                    value_sample_complete.get((tgt_tbl.full_name, tgt_col.name), False)
                                )
                            else:
                                effective_sample = (
                                    min(config.sample_size, 1000)
                                    if config.profile_mode == "fast"
                                    else config.sample_size
                                )
                                tgt_fully_sampled = (
                                    tgt_tbl.row_count is not None
                                    and tgt_tbl.row_count <= min(effective_sample, 2000)
                                    and not config.use_tablesample
                                )
                            if tgt_fully_sampled:
                                continue
                            final_score = round(final_score * 0.80, 3)

                if final_score < 0.65:
                    continue

                cand = RelationshipInfo(
                    source_table=src_tbl.full_name,
                    source_column=src_col.name,
                    target_table=tgt_tbl.full_name,
                    target_column=tgt_col.name,
                    kind=kind,  # type: ignore[arg-type]
                    confidence=round(final_score, 3),
                    overlap_rate=overlap_rate,
                    evidence=evidence,
                )
                cand_same_schema = tgt_tbl.schema_name.lower() == src_tbl.schema_name.lower()
                if (
                    best_candidate is None
                    or cand.confidence > best_candidate.confidence
                    or (cand.confidence == best_candidate.confidence and cand_same_schema and not best_same_schema)
                ):
                    best_candidate = cand
                    best_same_schema = cand_same_schema

            if best_candidate is not None:
                relationships.append(best_candidate)

    relationships.sort(key=lambda r: (r.kind != "explicit_fk", -r.confidence, r.source_table))
    return relationships, lineage_edges


def _extract_tables_from_sql(sql: str, sqlglot_dialect: str | None) -> set[tuple[str | None, str]]:
    """Parse view SQL with sqlglot (with regex fallback) to find referenced tables excluding CTEs."""
    refs: set[tuple[str | None, str]] = set()
    try:
        parsed_list = sqlglot.parse(sql, read=sqlglot_dialect)
        parsed_cleanly = bool(parsed_list) and all(
            stmt is not None and not isinstance(stmt, exp.Command) for stmt in parsed_list
        )
        for stmt in parsed_list:
            if stmt is None:
                continue
            cte_names = {
                cte.alias_or_name.lower()
                for cte in stmt.find_all(exp.CTE)
                if cte.alias_or_name
            }
            # Exclude the view/table target being created in CREATE VIEW <target> AS SELECT ...
            create_target_ids: set[int] = set()
            if isinstance(stmt, exp.Create) and stmt.this is not None:
                for tnode in stmt.this.find_all(exp.Table):
                    create_target_ids.add(id(tnode))
            for tbl in stmt.find_all(exp.Table):
                if id(tbl) in create_target_ids:
                    continue
                name = tbl.name
                if not name:
                    continue
                schema = tbl.db or None
                if schema is None and name.lower() in cte_names:
                    continue
                refs.add((schema.lower() if schema else None, name.lower()))
        if parsed_cleanly or refs:
            return refs
    except Exception:
        pass

    # Regex fallback for vendor-specific syntax
    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+(?:[\"`\[]?(\w+)[\"`\]]?\.)?[\"`\[]?(\w+)[\"`\]]?",
        sql,
        flags=re.IGNORECASE,
    ):
        schema, name = match.group(1), match.group(2)
        if name.lower() not in {"select", "lateral", "unnest", "values"}:
            refs.add((schema.lower() if schema else None, name.lower()))
    return refs


def discover_view_lineage(
    tables: list[TableInfo], adapter: DialectAdapter
) -> list[LineageEdge]:
    """Parse view definitions to populate upstream_tables and view dependency edges."""
    edges: list[LineageEdge] = []
    full_lookup = {t.full_name.lower(): t.full_name for t in tables}
    name_lookup: dict[str, list[str]] = {}
    for t in tables:
        name_lookup.setdefault(t.name.lower(), []).append(t.full_name)

    for tbl in tables:
        if tbl.table_type not in {"view", "materialized_view"} or not tbl.view_definition:
            continue

        raw_refs = _extract_tables_from_sql(tbl.view_definition, adapter.sqlglot_dialect)
        resolved: list[str] = []

        for ref_schema, ref_name in sorted(raw_refs):
            target: str | None = None
            if ref_schema:
                # Explicitly schema-qualified references must only resolve within that schema
                target = full_lookup.get(f"{ref_schema}.{ref_name}")
            else:
                same_schema_key = f"{tbl.schema_name.lower()}.{ref_name}"
                if same_schema_key in full_lookup:
                    target = full_lookup[same_schema_key]
                else:
                    matches = name_lookup.get(ref_name, [])
                    if len(matches) == 1:
                        target = matches[0]
            if target is None:
                target = f"{ref_schema}.{ref_name}" if ref_schema else ref_name

            if target != tbl.full_name and target not in resolved:
                resolved.append(target)

        tbl.upstream_tables = resolved
        for upstream in resolved:
            edges.append(
                LineageEdge(
                    upstream_table=upstream,
                    downstream_table=tbl.full_name,
                    edge_type="view_dependency",
                    details=f"{tbl.table_type} '{tbl.name}' reads from '{upstream}'",
                )
            )

    return edges
