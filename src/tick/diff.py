"""Compare two tick discovery JSON snapshots to detect schema and data drift."""

from __future__ import annotations

import json
import re
from pathlib import Path

from tick.models import ColumnChange, DiscoveryResult, MetricDrift, SnapshotDiff

_TYPE_ARGS_RE = re.compile(r"\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")
_INT_RANK = {
    "TINYINT": 1,
    "BYTEINT": 1,
    "UTINYINT": 1,
    "SMALLINT": 2,
    "INT2": 2,
    "SHORT": 2,
    "USMALLINT": 2,
    "MEDIUMINT": 3,
    "INT": 4,
    "INTEGER": 4,
    "INT4": 4,
    "INT32": 4,
    "UINTEGER": 4,
    "UINT32": 4,
    "SERIAL": 4,
    "BIGINT": 5,
    "INT8": 5,
    "INT64": 5,
    "UBIGINT": 5,
    "UINT64": 5,
    "BIGSERIAL": 5,
    "HUGEINT": 6,
    "INT128": 6,
}


def _is_breaking_type_change(
    before_raw: str,
    before_canon: str,
    after_raw: str,
    after_canon: str,
) -> bool:
    """Return True if a type change crosses canonical categories or narrows width/precision/scale."""
    if before_canon != after_canon:
        return True

    bu = before_raw.strip().upper()
    au = after_raw.strip().upper()
    b_base = re.split(r"[\s(]", bu, maxsplit=1)[0]
    a_base = re.split(r"[\s(]", au, maxsplit=1)[0]

    if before_canon == "integer":
        b_rank = _INT_RANK.get(b_base)
        a_rank = _INT_RANK.get(a_base)
        if b_rank is not None and a_rank is not None and a_rank < b_rank:
            return True
        bm, am = _TYPE_ARGS_RE.search(bu), _TYPE_ARGS_RE.search(au)
        if bm and am and int(am.group(1)) < int(bm.group(1)):
            return True

    if before_canon == "decimal":
        bm, am = _TYPE_ARGS_RE.search(bu), _TYPE_ARGS_RE.search(au)
        if bm and am:
            bp, bs = int(bm.group(1)), int(bm.group(2) or 0)
            ap, as_ = int(am.group(1)), int(am.group(2) or 0)
            if ap < bp or as_ < bs or (ap - as_) < (bp - bs):
                return True

    if before_canon == "string":
        bm, am = _TYPE_ARGS_RE.search(bu), _TYPE_ARGS_RE.search(au)
        # Unbounded TEXT/STRING narrowed to bounded VARCHAR(n), or VARCHAR(255) -> VARCHAR(50)
        if not bm and am and b_base in {"TEXT", "STRING", "CLOB", "NTEXT", "LONGTEXT", "MEDIUMTEXT"}:
            return True
        if bm and am and int(am.group(1)) < int(bm.group(1)):
            return True

    if before_canon == "float":
        if b_base in {"DOUBLE", "FLOAT8", "BINARY_DOUBLE"} and a_base in {"FLOAT", "FLOAT4", "REAL", "BINARY_FLOAT"}:
            return True

    return False


def load_snapshot(path: str | Path) -> DiscoveryResult:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return DiscoveryResult.model_validate(data)


def diff_snapshots(before: DiscoveryResult, after: DiscoveryResult) -> SnapshotDiff:
    """Compute breaking schema changes, PII additions, and metric drift between two snapshots."""
    before_tables = {t.full_name: t for t in before.tables}
    after_tables = {t.full_name: t for t in after.tables}

    added_tables = sorted(set(after_tables) - set(before_tables))
    removed_tables = sorted(set(before_tables) - set(after_tables))

    column_changes: list[ColumnChange] = []
    pk_changes: list[str] = []
    new_pii: list[str] = []
    metric_drifts: list[MetricDrift] = []

    for tbl_name in sorted(set(before_tables) & set(after_tables)):
        bt = before_tables[tbl_name]
        at = after_tables[tbl_name]

        # Primary key changes
        if bt.primary_key != at.primary_key:
            pk_changes.append(
                f"{tbl_name}: {bt.primary_key or ['<none>']} -> {at.primary_key or ['<none>']}"
            )

        # Row count drift
        if bt.row_count is not None and at.row_count is not None and bt.row_count != at.row_count:
            delta = at.row_count - bt.row_count
            pct = (delta / bt.row_count) if bt.row_count > 0 else None
            if (bt.row_count > 0 and at.row_count == 0) or (pct is not None and abs(pct) >= 0.20):
                sev = "high" if (bt.row_count > 0 and at.row_count == 0) else (
                    "medium" if (pct is not None and abs(pct) >= 0.50) else "low"
                )
                metric_drifts.append(
                    MetricDrift(
                        table=tbl_name,
                        metric="row_count",
                        before=bt.row_count,
                        after=at.row_count,
                        delta=float(delta),
                        pct_change=round(pct * 100.0, 2) if pct is not None else None,
                        severity=sev,  # type: ignore[arg-type]
                    )
                )

        b_cols = {c.name: c for c in bt.columns}
        a_cols = {c.name: c for c in at.columns}

        for cname in sorted(set(a_cols) - set(b_cols)):
            ac = a_cols[cname]
            column_changes.append(
                ColumnChange(
                    table=tbl_name,
                    column=cname,
                    change_type="added",
                    after=ac.raw_type,
                    breaking=False,
                )
            )
            if ac.profile and ac.profile.pii_findings:
                cats = ", ".join(f.category for f in ac.profile.pii_findings)
                new_pii.append(f"{tbl_name}.{cname} ({cats})")

        for cname in sorted(set(b_cols) - set(a_cols)):
            bc = b_cols[cname]
            column_changes.append(
                ColumnChange(
                    table=tbl_name,
                    column=cname,
                    change_type="removed",
                    before=bc.raw_type,
                    breaking=True,
                )
            )

        for cname in sorted(set(b_cols) & set(a_cols)):
            bc = b_cols[cname]
            ac = a_cols[cname]

            if bc.raw_type.upper() != ac.raw_type.upper():
                column_changes.append(
                    ColumnChange(
                        table=tbl_name,
                        column=cname,
                        change_type="type_changed",
                        before=bc.raw_type,
                        after=ac.raw_type,
                        breaking=_is_breaking_type_change(
                            bc.raw_type, bc.canonical_type, ac.raw_type, ac.canonical_type
                        ),
                    )
                )

            if bc.nullable != ac.nullable:
                column_changes.append(
                    ColumnChange(
                        table=tbl_name,
                        column=cname,
                        change_type="nullability_changed",
                        before="NULL" if bc.nullable else "NOT NULL",
                        after="NULL" if ac.nullable else "NOT NULL",
                        breaking=(not bc.nullable and ac.nullable),
                    )
                )

            # New PII on existing column
            b_pii = {f.category for f in (bc.profile.pii_findings if bc.profile else [])}
            a_pii = {f.category for f in (ac.profile.pii_findings if ac.profile else [])}
            added_pii = sorted(a_pii - b_pii)
            if added_pii:
                new_pii.append(f"{tbl_name}.{cname} ({', '.join(added_pii)})")

            # Null rate regression (>= +10pp) — only when both snapshots actually profiled the column
            if (
                bc.profile
                and ac.profile
                and not bc.profile.profile_error
                and not ac.profile.profile_error
                and bc.profile.sample_size > 0
                and ac.profile.sample_size > 0
            ):
                delta_null = ac.profile.null_rate - bc.profile.null_rate
                if delta_null >= 0.10:
                    metric_drifts.append(
                        MetricDrift(
                            table=tbl_name,
                            column=cname,
                            metric="null_rate",
                            before=round(bc.profile.null_rate * 100.0, 2),
                            after=round(ac.profile.null_rate * 100.0, 2),
                            delta=round(delta_null * 100.0, 2),
                            severity="high" if delta_null >= 0.30 else "medium",
                        )
                    )

    # New PII on newly added tables
    for tbl_name in added_tables:
        for col in after_tables[tbl_name].columns:
            if col.profile and col.profile.pii_findings:
                cats = ", ".join(f.category for f in col.profile.pii_findings)
                new_pii.append(f"{tbl_name}.{col.name} ({cats})")

    breaking_count = (
        len(removed_tables)
        + len(pk_changes)
        + sum(1 for c in column_changes if c.breaking)
    )

    return SnapshotDiff(
        before_discovered_at=before.overview.discovered_at,
        after_discovered_at=after.overview.discovered_at,
        before_database=before.overview.database_name,
        after_database=after.overview.database_name,
        added_tables=added_tables,
        removed_tables=removed_tables,
        column_changes=column_changes,
        primary_key_changes=pk_changes,
        new_pii_columns=sorted(set(new_pii)),
        metric_drifts=metric_drifts,
        breaking_changes_count=breaking_count,
        has_breaking_changes=breaking_count > 0,
    )
