"""Local & Data Lake file discovery (Parquet, CSV, TSV, JSON/JSONL, Hive partitions, Delta Lake)."""

from __future__ import annotations

import csv
import glob
import gzip
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from tick.dialects import DriverMissingError, _module_installed
from tick.models import DiscoveryConfig, FileSourceInfo

FileFormat = Literal["parquet", "csv", "tsv", "json", "jsonl", "delta"]

_SUPPORTED_SIMPLE_EXTS: dict[str, tuple[FileFormat, str | None]] = {
    ".parquet": ("parquet", None),
    ".pq": ("parquet", None),
    ".csv": ("csv", None),
    ".tsv": ("tsv", None),
    ".json": ("json", None),
    ".jsonl": ("jsonl", None),
    ".ndjson": ("jsonl", None),
}
_SUPPORTED_GZ_EXTS: dict[str, tuple[FileFormat, str]] = {
    ".csv.gz": ("csv", "gzip"),
    ".tsv.gz": ("tsv", "gzip"),
    ".json.gz": ("json", "gzip"),
    ".jsonl.gz": ("jsonl", "gzip"),
    ".ndjson.gz": ("jsonl", "gzip"),
}
_HIVE_PART_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=([^/\\]+)$")
_SHARD_NAME_RE = re.compile(r"^(?:part[-_]\d+|data[-_]\d+|\d{4,}|.+[-_]\d{3,})$", re.I)
_IGNORED_FILENAMES = {".ds_store", "_success", "_committed", "_started"}
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")


def _detect_file_format(path: Path) -> tuple[FileFormat, str | None] | None:
    name_l = path.name.lower()
    if name_l.startswith(".") or name_l.startswith("_") or name_l in _IGNORED_FILENAMES:
        return None
    for ext, info in _SUPPORTED_GZ_EXTS.items():
        if name_l.endswith(ext):
            return info
    return _SUPPORTED_SIMPLE_EXTS.get(path.suffix.lower())


def _stem_without_format(path: Path) -> str:
    name = path.name
    low = name.lower()
    for ext in _SUPPORTED_GZ_EXTS:
        if low.endswith(ext):
            return name[: -len(ext)]
    return path.stem


def _sanitize_sql_ident(name: str, fallback: str = "dataset") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", name.strip()).strip("_")
    if not cleaned:
        cleaned = fallback
    if cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned


def _has_glob_magic(target: str) -> bool:
    return any(ch in target for ch in ("*", "?", "["))


def is_file_lake_target(target: str) -> bool:
    """Return True if target is a flat data file, directory of data files, or file glob pattern."""
    stripped = target.strip()
    if not stripped or stripped == ":memory:" or "://" in stripped:
        return False
    if _has_glob_magic(stripped):
        return True
    p = Path(stripped).expanduser()
    if p.is_dir():
        return True
    if _detect_file_format(p) is not None:
        return True
    return False


def _sniff_csv_props(path: Path, is_tsv: bool, compression: str | None) -> tuple[str, bool]:
    """Inspect the first 8 KB of a CSV/TSV file to detect delimiter and header row."""
    default_delim = "\t" if is_tsv else ","
    try:
        opener = gzip.open if compression == "gzip" else open
        with opener(path, "rt", encoding="utf-8", errors="replace") as fh:
            sample = fh.read(8192)
        if not sample.strip():
            return default_delim, True
        delim = default_delim
        if not is_tsv:
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
                delim = dialect.delimiter
            except csv.Error:
                delim = ","
        has_header = True
        try:
            has_header = csv.Sniffer().has_header(sample)
        except csv.Error:
            has_header = True
        return delim, has_header
    except Exception:
        return default_delim, True


def _detect_json_lines(path: Path, compression: str | None, fallback_fmt: FileFormat) -> FileFormat:
    """Distinguish JSON array vs newline-delimited JSON (JSONL/NDJSON) from first non-whitespace byte."""
    if fallback_fmt == "jsonl":
        return "jsonl"
    try:
        opener = gzip.open if compression == "gzip" else open
        with opener(path, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                s = line.strip()
                if not s:
                    continue
                if s.startswith("["):
                    return "json"
                if s.startswith("{"):
                    # Check if a second JSON object line follows -> JSONL
                    for next_line in fh:
                        ns = next_line.strip()
                        if ns:
                            return "jsonl" if ns.startswith("{") else "json"
                    return "jsonl"
                break
    except Exception:
        pass
    return fallback_fmt


@dataclass
class DiscoveredFileDataset:
    schema_name: str
    table_name: str
    source_paths: list[Path]
    read_glob: str | None
    file_source: FileSourceInfo


@dataclass
class MountedFileLake:
    engine: Engine
    resolved_url: str
    database_name: str
    backend_note: str
    file_sources: dict[str, FileSourceInfo] = field(default_factory=dict)
    exact_row_counts: dict[str, int] = field(default_factory=dict)
    size_bytes_map: dict[str, int] = field(default_factory=dict)


def _list_data_files(directory: Path, *, ignore_delta_log: bool = True) -> list[tuple[Path, FileFormat, str | None]]:
    found: list[tuple[Path, FileFormat, str | None]] = []
    for child in sorted(directory.rglob("*")):
        if not child.is_file():
            continue
        rel_parts = child.relative_to(directory).parts
        if ignore_delta_log and "_delta_log" in rel_parts:
            continue
        if any(part.startswith(".") for part in rel_parts):
            continue
        fmt_info = _detect_file_format(child)
        if fmt_info is not None:
            found.append((child, fmt_info[0], fmt_info[1]))
    return found


def _extract_hive_partition_cols(root: Path, files: list[Path]) -> list[str]:
    cols: list[str] = []
    for f in files:
        try:
            rel_parents = f.relative_to(root).parts[:-1]
        except ValueError:
            continue
        for part in rel_parents:
            m = _HIVE_PART_RE.match(part)
            if m and m.group(1) not in cols:
                cols.append(m.group(1))
    return cols


def _looks_like_sharded_table(files: list[tuple[Path, FileFormat, str | None]]) -> bool:
    if len(files) < 2:
        return False
    fmts = {fmt for _, fmt, _ in files}
    if len(fmts) != 1:
        return False
    stems = [_stem_without_format(p) for p, _, _ in files]
    if all(_SHARD_NAME_RE.match(s) for s in stems):
        return True
    # Common numeric suffix pattern like orders_001.parquet, orders_002.parquet
    prefixes = {re.sub(r"[-_]?\d+$", "", s).lower() for s in stems}
    if len(prefixes) == 1 and "" not in prefixes:
        return True
    return False


def _build_dataset(
    schema_name: str,
    raw_table_name: str,
    files: list[Path],
    fmt: FileFormat,
    compression: str | None,
    display_path: str,
    read_glob: str | None = None,
    hive_partitioned: bool = False,
    partition_columns: list[str] | None = None,
) -> DiscoveredFileDataset:
    total_bytes = sum(p.stat().st_size for p in files if p.exists())
    actual_fmt = fmt
    delimiter: str | None = None
    has_header: bool | None = None
    if fmt in {"csv", "tsv"} and files:
        delimiter, has_header = _sniff_csv_props(files[0], fmt == "tsv", compression)
    elif fmt in {"json", "jsonl"} and files:
        actual_fmt = _detect_json_lines(files[0], compression, fmt)

    return DiscoveredFileDataset(
        schema_name=_sanitize_sql_ident(schema_name, fallback="files"),
        table_name=_sanitize_sql_ident(raw_table_name, fallback="dataset"),
        source_paths=files,
        read_glob=read_glob,
        file_source=FileSourceInfo(
            path=display_path,
            file_format=actual_fmt,
            file_count=len(files),
            total_bytes=total_bytes,
            compression=compression,
            delimiter=delimiter,
            has_header=has_header,
            hive_partitioned=hive_partitioned,
            partition_columns=list(partition_columns or []),
        ),
    )


def _deduplicate_datasets(datasets: list[DiscoveredFileDataset]) -> list[DiscoveredFileDataset]:
    seen: set[tuple[str, str]] = set()
    out: list[DiscoveredFileDataset] = []
    for ds in datasets:
        base = ds.table_name
        cand = base
        suffix = 2
        while (ds.schema_name.lower(), cand.lower()) in seen:
            cand = f"{base}_{suffix}"
            suffix += 1
        seen.add((ds.schema_name.lower(), cand.lower()))
        ds.table_name = cand
        out.append(ds)
    return out


def scan_file_sources(target: str) -> tuple[str, str, list[DiscoveredFileDataset]]:
    """Scan a file, directory landing zone, or glob pattern into logical table datasets.

    Returns `(database_name, display_url, datasets)`.
    """
    stripped = target.strip()

    # 1. Glob pattern (e.g. "./drop/*.csv" or "/data/**/*.parquet")
    if _has_glob_magic(stripped):
        expanded = str(Path(stripped).expanduser())
        matched_paths = sorted(Path(p).resolve() for p in glob.glob(expanded, recursive=True) if Path(p).is_file())
        valid: list[tuple[Path, FileFormat, str | None]] = []
        for p in matched_paths:
            fmt_info = _detect_file_format(p)
            if fmt_info:
                valid.append((p, fmt_info[0], fmt_info[1]))
        if not valid:
            raise FileNotFoundError(f"No supported data files (.parquet, .csv, .tsv, .json, .jsonl) matched '{target}'.")

        base_dir = Path(expanded).parent
        db_name = _sanitize_sql_ident(base_dir.name or "file_glob", fallback="file_glob")
        if _looks_like_sharded_table(valid):
            files = [p for p, _, _ in valid]
            fmt, comp = valid[0][1], valid[0][2]
            part_cols = _extract_hive_partition_cols(base_dir.resolve(), files) if base_dir.exists() else []
            ds = _build_dataset(
                "files",
                db_name,
                files,
                fmt,
                comp,
                display_path=str(Path(expanded)),
                read_glob=str(Path(expanded)),
                hive_partitioned=bool(part_cols),
                partition_columns=part_cols,
            )
            return db_name, f"files://{expanded}", [ds]

        datasets = [
            _build_dataset("files", _stem_without_format(p), [p], fmt, comp, display_path=str(p))
            for p, fmt, comp in valid
        ]
        return db_name, f"files://{expanded}", _deduplicate_datasets(datasets)

    path = Path(stripped).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"File or directory not found: '{path}'")

    # 2. Single file
    if path.is_file():
        fmt_info = _detect_file_format(path)
        if fmt_info is None:
            raise ValueError(
                f"Unsupported file format '{path.name}'. Supported: .parquet, .pq, .csv, .tsv, .json, .jsonl, .ndjson (and .gz)."
            )
        stem = _stem_without_format(path)
        ds = _build_dataset("files", stem, [path], fmt_info[0], fmt_info[1], display_path=str(path))
        return path.name, f"file://{path}", [ds]

    # 3. Directory (could be a single Delta/Hive/sharded table OR a landing zone with files + schema dirs)
    db_name = path.name or "landing_zone"
    if (path / "_delta_log").is_dir():
        parquet_files = [p for p, fmt, _ in _list_data_files(path, ignore_delta_log=True) if fmt == "parquet"]
        if not parquet_files:
            raise FileNotFoundError(f"Delta directory '{path}' contains no readable Parquet data files.")
        part_cols = _extract_hive_partition_cols(path, parquet_files)
        ds = _build_dataset(
            "files",
            path.name,
            parquet_files,
            "delta",
            None,
            display_path=str(path),
            read_glob=str(path / "**" / "*.parquet"),
            hive_partitioned=bool(part_cols),
            partition_columns=part_cols,
        )
        return db_name, f"files://{path}", [ds]

    all_root_files = _list_data_files(path, ignore_delta_log=True)
    if not all_root_files:
        raise FileNotFoundError(
            f"Directory '{path}' contains no supported data files (.parquet, .csv, .tsv, .json, .jsonl)."
        )

    # Check if the root directory ITSELF is a single Hive-partitioned table (only k=v subdirs)
    direct_subdirs = sorted(c for c in path.iterdir() if c.is_dir() and not c.name.startswith((".", "_")))
    direct_files = sorted(c for c in path.iterdir() if c.is_file())
    direct_data_files = [
        (c, fmt_info[0], fmt_info[1])
        for c in direct_files
        if (fmt_info := _detect_file_format(c)) is not None
    ]

    if not direct_data_files and direct_subdirs and all(_HIVE_PART_RE.match(d.name) for d in direct_subdirs):
        fmts = {fmt for _, fmt, _ in all_root_files}
        if len(fmts) == 1:
            files = [p for p, _, _ in all_root_files]
            fmt, comp = all_root_files[0][1], all_root_files[0][2]
            part_cols = _extract_hive_partition_cols(path, files)
            ds = _build_dataset(
                "files",
                path.name,
                files,
                fmt,
                comp,
                display_path=str(path),
                hive_partitioned=True,
                partition_columns=part_cols,
            )
            return db_name, f"files://{path}", [ds]

    if not direct_subdirs and _looks_like_sharded_table(direct_data_files):
        files = [p for p, _, _ in direct_data_files]
        fmt, comp = direct_data_files[0][1], direct_data_files[0][2]
        ds = _build_dataset("files", path.name, files, fmt, comp, display_path=str(path))
        return db_name, f"files://{path}", [ds]

    datasets: list[DiscoveredFileDataset] = []
    # Direct files in root -> schema "files"
    for fpath, fmt, comp in direct_data_files:
        datasets.append(_build_dataset("files", _stem_without_format(fpath), [fpath], fmt, comp, display_path=str(fpath)))

    # Subdirectories -> either Delta table, Hive-partitioned table, sharded table, or schema folder
    for subdir in direct_subdirs:
        if (subdir / "_delta_log").is_dir():
            pfiles = [p for p, fmt, _ in _list_data_files(subdir, ignore_delta_log=True) if fmt == "parquet"]
            if pfiles:
                part_cols = _extract_hive_partition_cols(subdir, pfiles)
                datasets.append(
                    _build_dataset(
                        "files",
                        subdir.name,
                        pfiles,
                        "delta",
                        None,
                        display_path=str(subdir),
                        hive_partitioned=bool(part_cols),
                        partition_columns=part_cols,
                    )
                )
            continue

        sub_all = _list_data_files(subdir, ignore_delta_log=True)
        if not sub_all:
            continue

        sub_children_dirs = [c for c in subdir.iterdir() if c.is_dir() and not c.name.startswith((".", "_"))]
        is_hive_dir = bool(sub_children_dirs) and all(_HIVE_PART_RE.match(d.name) for d in sub_children_dirs)
        sub_fmts = {fmt for _, fmt, _ in sub_all}

        if is_hive_dir and len(sub_fmts) == 1:
            files = [p for p, _, _ in sub_all]
            fmt, comp = sub_all[0][1], sub_all[0][2]
            part_cols = _extract_hive_partition_cols(subdir, files)
            datasets.append(
                _build_dataset(
                    "files",
                    subdir.name,
                    files,
                    fmt,
                    comp,
                    display_path=str(subdir),
                    hive_partitioned=True,
                    partition_columns=part_cols,
                )
            )
        elif not sub_children_dirs and _looks_like_sharded_table(sub_all):
            files = [p for p, _, _ in sub_all]
            fmt, comp = sub_all[0][1], sub_all[0][2]
            datasets.append(_build_dataset("files", subdir.name, files, fmt, comp, display_path=str(subdir)))
        else:
            # Identify nested Delta / Hive subdirs so we don't also emit their inner shard files
            nested_special_dirs: set[Path] = set()
            for nested in sorted(sub_children_dirs):
                if (nested / "_delta_log").is_dir():
                    nested_special_dirs.add(nested)
                    pfiles = [p for p, fmt, _ in _list_data_files(nested, ignore_delta_log=True) if fmt == "parquet"]
                    if pfiles:
                        pcols = _extract_hive_partition_cols(nested, pfiles)
                        datasets.append(
                            _build_dataset(
                                subdir.name,
                                nested.name,
                                pfiles,
                                "delta",
                                None,
                                display_path=str(nested),
                                hive_partitioned=bool(pcols),
                                partition_columns=pcols,
                            )
                        )
                else:
                    ndirs = [c for c in nested.iterdir() if c.is_dir() and not c.name.startswith((".", "_"))]
                    if ndirs and all(_HIVE_PART_RE.match(d.name) for d in ndirs):
                        nfiles_all = _list_data_files(nested, ignore_delta_log=True)
                        if nfiles_all and len({fmt for _, fmt, _ in nfiles_all}) == 1:
                            nested_special_dirs.add(nested)
                            nfiles = [p for p, _, _ in nfiles_all]
                            pcols = _extract_hive_partition_cols(nested, nfiles)
                            datasets.append(
                                _build_dataset(
                                    subdir.name,
                                    nested.name,
                                    nfiles,
                                    nfiles_all[0][1],
                                    nfiles_all[0][2],
                                    display_path=str(nested),
                                    hive_partitioned=True,
                                    partition_columns=pcols,
                                )
                            )

            # Treat subdir as a logical schema (e.g., sap/MARA.csv, mes/work_orders.parquet)
            for fpath, fmt, comp in sub_all:
                if any(spec in fpath.parents for spec in nested_special_dirs):
                    continue
                datasets.append(
                    _build_dataset(
                        subdir.name,
                        _stem_without_format(fpath),
                        [fpath],
                        fmt,
                        comp,
                        display_path=str(fpath),
                    )
                )

    if not datasets:
        raise FileNotFoundError(f"No readable datasets found under '{path}'.")

    return db_name, f"files://{path}", _deduplicate_datasets(datasets)


def _duckdb_sql_str(val: str) -> str:
    return "'" + val.replace("'", "''") + "'"


def _duckdb_read_expr(ds: DiscoveredFileDataset) -> str:
    fs = ds.file_source
    paths_sql = "[" + ", ".join(_duckdb_sql_str(str(p)) for p in ds.source_paths) + "]"

    if fs.file_format in {"parquet", "delta"}:
        opts = ["union_by_name = true"]
        if fs.hive_partitioned:
            opts.append("hive_partitioning = true")
        return f"read_parquet({paths_sql}, {', '.join(opts)})"

    if fs.file_format in {"csv", "tsv"}:
        opts = ["union_by_name = true", "ignore_errors = false"]
        if fs.delimiter:
            opts.append(f"delim = {_duckdb_sql_str(fs.delimiter)}")
        if fs.has_header is not None:
            opts.append(f"header = {'true' if fs.has_header else 'false'}")
        if fs.hive_partitioned:
            opts.append("hive_partitioning = true")
        return f"read_csv_auto({paths_sql}, {', '.join(opts)})"

    # json / jsonl
    opts = ["union_by_name = true"]
    if fs.hive_partitioned:
        opts.append("hive_partitioning = true")
    return f"read_json_auto({paths_sql}, {', '.join(opts)})"


def _mount_via_duckdb(
    db_name: str,
    resolved_url: str,
    datasets: list[DiscoveredFileDataset],
    config: DiscoveryConfig,
) -> MountedFileLake:
    engine = create_engine("duckdb:///:memory:", future=True, poolclass=StaticPool)
    max_materialize = max(config.sample_size * 4, 50_000)

    mounted = MountedFileLake(
        engine=engine,
        resolved_url=resolved_url,
        database_name=db_name,
        backend_note="Mounted via DuckDB in-memory file engine",
    )

    with engine.begin() as conn:
        for ds in datasets:
            sch_q = '"' + ds.schema_name.replace('"', '""') + '"'
            tbl_q = '"' + ds.table_name.replace('"', '""') + '"'
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {sch_q}"))
            read_expr = _duckdb_read_expr(ds)

            try:
                exact_rows = int(conn.execute(text(f"SELECT COUNT(*) FROM {read_expr}")).scalar() or 0)
            except Exception:
                # Retry CSV with ignore_errors=true if dirty rows exist
                if ds.file_source.file_format in {"csv", "tsv"}:
                    read_expr = read_expr.replace("ignore_errors = false", "ignore_errors = true")
                    exact_rows = int(conn.execute(text(f"SELECT COUNT(*) FROM {read_expr}")).scalar() or 0)
                else:
                    raise

            limit_clause = f" LIMIT {max_materialize}" if exact_rows > max_materialize else ""
            conn.execute(text(f"CREATE TABLE {sch_q}.{tbl_q} AS SELECT * FROM {read_expr}{limit_clause}"))

            full_name = f"{ds.schema_name}.{ds.table_name}"
            mounted.file_sources[full_name] = ds.file_source
            mounted.exact_row_counts[full_name] = exact_rows
            mounted.size_bytes_map[full_name] = ds.file_source.total_bytes

    return mounted


def _infer_sqlite_type(values: list[Any]) -> str:
    non_null = [v for v in values if v is not None and str(v).strip() != ""]
    if not non_null:
        return "VARCHAR"
    if all(isinstance(v, bool) or str(v).strip().lower() in {"true", "false"} for v in non_null):
        return "BOOLEAN"
    all_int = True
    for v in non_null:
        if isinstance(v, bool):
            all_int = False
            break
        try:
            int(str(v).strip())
        except ValueError:
            all_int = False
            break
    if all_int:
        return "BIGINT"
    all_float = True
    for v in non_null:
        if isinstance(v, bool):
            all_float = False
            break
        try:
            float(str(v).strip())
        except ValueError:
            all_float = False
            break
    if all_float:
        return "DOUBLE"
    if all(_TS_RE.match(str(v).strip()) for v in non_null[:100]):
        return "TIMESTAMP"
    if all(_DATE_RE.match(str(v).strip()) for v in non_null[:100]):
        return "DATE"
    return "VARCHAR"


def _read_rows_python(ds: DiscoveredFileDataset, max_rows: int) -> tuple[list[str], list[dict[str, Any]], int]:
    """Pure-Python CSV/TSV/JSON/JSONL reader for zero-dependency SQLite fallback."""
    fs = ds.file_source
    rows: list[dict[str, Any]] = []
    columns: list[str] = []
    total_rows = 0

    for fpath in ds.source_paths:
        # Extract Hive partition key=val pairs from parent directories when hive_partitioned
        part_vals: dict[str, str] = {}
        if fs.hive_partitioned:
            for part in fpath.parts[:-1]:
                m = _HIVE_PART_RE.match(part)
                if m and (not fs.partition_columns or m.group(1) in fs.partition_columns):
                    part_vals[m.group(1)] = m.group(2)
            for pk in part_vals:
                if pk not in columns:
                    columns.append(pk)

        opener = gzip.open if fs.compression == "gzip" else open
        if fs.file_format in {"csv", "tsv"}:
            delim = fs.delimiter or ("\t" if fs.file_format == "tsv" else ",")
            with opener(fpath, "rt", encoding="utf-8", errors="replace", newline="") as fh:
                reader = csv.reader(fh, delimiter=delim)
                header = next(reader, None)
                if header is None:
                    continue
                if fs.has_header is False:
                    col_names = [f"column{i}" for i in range(len(header))]
                    first_row = header
                    for c in col_names:
                        if c not in columns:
                            columns.append(c)
                    total_rows += 1
                    if len(rows) < max_rows:
                        row_dict = {col_names[i]: (first_row[i] if i < len(first_row) else None) for i in range(len(col_names))}
                        row_dict.update(part_vals)
                        rows.append(row_dict)
                else:
                    col_names = [h.strip() or f"column{i}" for i, h in enumerate(header)]
                    for c in col_names:
                        if c not in columns:
                            columns.append(c)
                for rec in reader:
                    if not rec:
                        continue
                    total_rows += 1
                    if len(rows) < max_rows:
                        row_dict = {col_names[i]: (rec[i] if i < len(rec) and rec[i] != "" else None) for i in range(len(col_names))}
                        row_dict.update(part_vals)
                        rows.append(row_dict)
        else:
            with opener(fpath, "rt", encoding="utf-8", errors="replace") as fh:
                if fs.file_format == "json":
                    data = json.load(fh)
                    items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
                else:
                    items = []
                    for line in fh:
                        s = line.strip()
                        if s:
                            items.append(json.loads(s))
                for obj in items:
                    if not isinstance(obj, dict):
                        continue
                    total_rows += 1
                    for k in obj:
                        if str(k) not in columns:
                            columns.append(str(k))
                    if len(rows) < max_rows:
                        row_dict = {
                            str(k): (json.dumps(v) if isinstance(v, (dict, list)) else v)
                            for k, v in obj.items()
                        }
                        row_dict.update(part_vals)
                        rows.append(row_dict)

    return columns, rows, total_rows


def _mount_via_sqlite_fallback(
    db_name: str,
    resolved_url: str,
    datasets: list[DiscoveredFileDataset],
    config: DiscoveryConfig,
) -> MountedFileLake:
    if any(ds.file_source.file_format in {"parquet", "delta"} for ds in datasets):
        raise DriverMissingError(
            "Reading Parquet or Delta Lake files requires DuckDB. Install it with:\n"
            '  pip install "tick-db[duckdb]"'
        )

    engine = create_engine("sqlite:///:memory:", future=True, poolclass=StaticPool)
    max_materialize = max(config.sample_size * 4, 25_000)

    mounted = MountedFileLake(
        engine=engine,
        resolved_url=resolved_url,
        database_name=db_name,
        backend_note="Mounted via Python/SQLite fallback file engine (install tick-db[duckdb] for Parquet/Delta)",
    )

    with engine.begin() as conn:
        seen_tables: set[str] = set()
        for ds in datasets:
            # SQLite uses a single default schema ("main"); prefix table name if subdir schema differs
            tbl_name = ds.table_name if ds.schema_name == "files" else f"{ds.schema_name}_{ds.table_name}"
            base = tbl_name
            sfx = 2
            while tbl_name.lower() in seen_tables:
                tbl_name = f"{base}_{sfx}"
                sfx += 1
            seen_tables.add(tbl_name.lower())

            cols, rows, exact_rows = _read_rows_python(ds, max_materialize)
            if not cols:
                cols = ["value"]

            col_defs: list[str] = []
            for cname in cols:
                inferred = _infer_sqlite_type([r.get(cname) for r in rows[:200]])
                cq = '"' + cname.replace('"', '""') + '"'
                col_defs.append(f"{cq} {inferred}")

            tq = '"' + tbl_name.replace('"', '""') + '"'
            conn.execute(text(f"CREATE TABLE {tq} ({', '.join(col_defs)})"))

            if rows:
                placeholders = ", ".join(f":p{i}" for i in range(len(cols)))
                qcols = ", ".join('"' + c.replace('"', '""') + '"' for c in cols)
                insert_sql = text(f"INSERT INTO {tq} ({qcols}) VALUES ({placeholders})")
                payloads = [
                    {f"p{i}": r.get(c) for i, c in enumerate(cols)}
                    for r in rows
                ]
                conn.execute(insert_sql, payloads)

            full_name = f"main.{tbl_name}"
            mounted.file_sources[full_name] = ds.file_source
            mounted.exact_row_counts[full_name] = exact_rows
            mounted.size_bytes_map[full_name] = ds.file_source.total_bytes

    return mounted


def mount_file_lake_engine(
    target: str,
    config: DiscoveryConfig,
    *,
    prefer_duckdb: bool = True,
) -> MountedFileLake:
    """Scan local files/folders/globs and mount them into an in-memory SQLAlchemy engine for full discovery."""
    db_name, resolved_url, datasets = scan_file_sources(target)
    if prefer_duckdb and _module_installed("duckdb_engine") and _module_installed("duckdb"):
        return _mount_via_duckdb(db_name, resolved_url, datasets, config)
    return _mount_via_sqlite_fallback(db_name, resolved_url, datasets, config)
