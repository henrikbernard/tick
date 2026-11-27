"""Self-contained interactive HTML dashboard for multi-plant schema harmonization."""

from __future__ import annotations

import html
import json
from pathlib import Path

from tick.models import HarmonizationReport

HARMONIZE_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>tick Harmonize — __TITLE__</title>
<style>
  :root {
    --bg: #0b0f17;
    --bg-elev: #121826;
    --bg-card: #161e2e;
    --bg-hover: #1e293b;
    --border: #243047;
    --text: #e2e8f0;
    --text-muted: #94a3b8;
    --accent: #38bdf8;
    --accent-soft: rgba(56, 189, 248, 0.14);
    --green: #34d399;
    --green-soft: rgba(52, 211, 153, 0.15);
    --amber: #fbbf24;
    --amber-soft: rgba(251, 191, 36, 0.15);
    --red: #f87171;
    --red-soft: rgba(248, 113, 113, 0.16);
    --purple: #a78bfa;
    --purple-soft: rgba(167, 139, 250, 0.16);
    --orange: #fb923c;
    --orange-soft: rgba(251, 146, 60, 0.16);
    --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    --sans: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }
  [data-theme="light"] {
    --bg: #f8fafc;
    --bg-elev: #ffffff;
    --bg-card: #ffffff;
    --bg-hover: #f1f5f9;
    --border: #e2e8f0;
    --text: #0f172a;
    --text-muted: #64748b;
    --accent: #0284c7;
    --accent-soft: rgba(2, 132, 199, 0.1);
    --green: #059669;
    --green-soft: rgba(5, 150, 105, 0.12);
    --amber: #d97706;
    --amber-soft: rgba(217, 119, 6, 0.12);
    --red: #dc2626;
    --red-soft: rgba(220, 38, 38, 0.12);
    --purple: #7c3aed;
    --purple-soft: rgba(124, 58, 237, 0.12);
    --orange: #ea580c;
    --orange-soft: rgba(234, 88, 12, 0.12);
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: var(--sans); background: var(--bg); color: var(--text); font-size: 14px; line-height: 1.45; }
  header {
    position: sticky; top: 0; z-index: 40;
    background: var(--bg-elev); border-bottom: 1px solid var(--border);
    padding: 12px 22px; display: flex; align-items: center; justify-content: space-between; gap: 14px; flex-wrap: wrap;
  }
  .brand { display: flex; align-items: center; gap: 12px; }
  .logo {
    width: 32px; height: 32px; border-radius: 8px; background: var(--accent-soft);
    border: 1px solid var(--accent); color: var(--accent); font-weight: 800;
    display: grid; place-items: center; font-family: var(--mono); font-size: 15px;
  }
  .brand h1 { margin: 0; font-size: 16px; font-weight: 700; }
  .brand .sub { font-size: 12px; color: var(--text-muted); font-family: var(--mono); }
  .actions { display: flex; gap: 8px; flex-wrap: wrap; }
  .btn {
    background: var(--bg-card); color: var(--text); border: 1px solid var(--border);
    padding: 6px 11px; border-radius: 7px; cursor: pointer; font-size: 12px; font-weight: 600;
  }
  .btn:hover { background: var(--bg-hover); }
  .btn-primary { background: var(--accent-soft); color: var(--accent); border-color: rgba(56,189,248,0.45); }
  main { padding: 20px 24px; max-width: 1580px; margin: 0 auto; }
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; margin-bottom: 18px; }
  .kpi { background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
  .kpi .label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-muted); }
  .kpi .val { font-size: 22px; font-weight: 700; margin-top: 4px; font-family: var(--mono); }
  .kpi .hint { font-size: 12px; color: var(--text-muted); margin-top: 2px; }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 980px) { .grid-2 { grid-template-columns: 1fr; } }
  .card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; margin-bottom: 16px; }
  .card h2 { margin: 0 0 12px 0; font-size: 15px; font-weight: 700; }
  .mono { font-family: var(--mono); font-size: 12px; }
  .badge {
    display: inline-block; padding: 2px 7px; border-radius: 999px; font-size: 11px;
    font-weight: 600; font-family: var(--mono); white-space: nowrap; margin-right: 4px; margin-bottom: 2px;
  }
  .b-accent { background: var(--accent-soft); color: var(--accent); }
  .b-green { background: var(--green-soft); color: var(--green); }
  .b-amber { background: var(--amber-soft); color: var(--amber); }
  .b-red { background: var(--red-soft); color: var(--red); }
  .b-purple { background: var(--purple-soft); color: var(--purple); }
  .b-orange { background: var(--orange-soft); color: var(--orange); }
  .b-muted { background: var(--bg-hover); color: var(--text-muted); }

  .donut-wrap { display: flex; align-items: center; gap: 18px; flex-wrap: wrap; }
  .legend-list { display: flex; flex-direction: column; gap: 7px; flex: 1; min-width: 150px; }
  .legend-row { display: flex; align-items: center; justify-content: space-between; font-size: 12.5px; gap: 8px; }
  .legend-swatch { width: 10px; height: 10px; border-radius: 3px; display: inline-block; margin-right: 7px; }

  .filters { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; }
  .search-input {
    padding: 7px 10px; border-radius: 7px; border: 1px solid var(--border);
    background: var(--bg); color: var(--text); font-size: 13px; min-width: 220px;
  }
  .pill-btn {
    background: var(--bg); color: var(--text-muted); border: 1px solid var(--border);
    padding: 6px 10px; border-radius: 999px; cursor: pointer; font-size: 12px; font-weight: 600;
  }
  .pill-btn.active { background: var(--accent-soft); color: var(--accent); border-color: rgba(56,189,248,0.45); }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 9px 10px; border-bottom: 1px solid var(--border); vertical-align: middle; }
  th { color: var(--text-muted); font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; background: var(--bg-elev); position: sticky; top: 0; }
  tr.matrix-row { cursor: pointer; }
  tr.matrix-row:hover td { background: var(--bg-hover); }
  tr.matrix-row.selected td { background: var(--accent-soft); }

  .heat-cell {
    display: inline-block; min-width: 82px; text-align: center; padding: 4px 8px;
    border-radius: 6px; font-size: 11px; font-weight: 700; font-family: var(--mono);
  }
  .hc-aligned { background: var(--green-soft); color: var(--green); }
  .hc-conflict { background: var(--amber-soft); color: var(--amber); }
  .hc-site { background: var(--orange-soft); color: var(--orange); }
  .hc-pk { background: var(--purple-soft); color: var(--purple); }
  .hc-missing { background: var(--red-soft); color: var(--red); }

  pre.code-block {
    background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
    padding: 12px; overflow-x: auto; font-family: var(--mono); font-size: 12px; line-height: 1.5;
    max-height: 440px; overflow-y: auto;
  }
  .check-pill { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; color: var(--text-muted); cursor: pointer; }

  @media print {
    :root, [data-theme="dark"], [data-theme="light"] {
      --bg: #ffffff; --bg-elev: #f8fafc; --bg-card: #ffffff; --bg-hover: #f1f5f9;
      --border: #cbd5e1; --text: #0f172a; --text-muted: #475569;
    }
    header .actions, .filters { display: none !important; }
    header { position: static; border-bottom: 2px solid #0f172a; }
    .card { break-inside: avoid; }
  }
</style>
</head>
<body>
<header>
  <div class="brand">
    <div class="logo">tk</div>
    <div>
      <h1>Multi-Plant Schema Harmonization</h1>
      <div class="sub" id="hdr-sub"></div>
    </div>
  </div>
  <div class="actions">
    <button class="btn btn-primary" id="download-all-sql">Download All Unified SQL</button>
    <button class="btn" id="print-btn">Print / PDF</button>
    <button class="btn" id="theme-toggle">Light / Dark</button>
  </div>
</header>

<main>
  <div class="kpi-grid" id="kpi-grid"></div>

  <div class="grid-2">
    <div class="card">
      <h2>Cross-Plant Schema Alignment</h2>
      <div id="chart-align-donut"></div>
    </div>
    <div class="card">
      <h2>Drift Category Breakdown</h2>
      <div id="chart-drift-bars"></div>
    </div>
  </div>

  <div class="card">
    <h2>Plant &times; Table Presence &amp; Drift Heatmap</h2>
    <div class="filters">
      <input id="tbl-search" class="search-input" type="text" placeholder="Search table or column..." />
      <button class="pill-btn active" data-filter="all">All Tables</button>
      <button class="pill-btn" data-filter="drifted">Drifted Only</button>
      <button class="pill-btn" data-filter="conflicts">Type Conflicts</button>
      <button class="pill-btn" data-filter="site_cols">Site-Only Cols</button>
      <button class="pill-btn" data-filter="pk">PK Divergent</button>
      <button class="pill-btn" data-filter="missing">Missing Tables</button>
    </div>
    <div style="overflow-x:auto"><table id="matrix-table"></table></div>
  </div>

  <div id="inspector-host"></div>
</main>

<script id="tick-harmonize-data" type="application/json">__PAYLOAD_JSON__</script>
<script>
(function() {
  const payload = JSON.parse(document.getElementById("tick-harmonize-data").textContent);
  const report = payload.report;
  const allSql = payload.all_sql;

  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));

  function downloadBlob(filename, content, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = filename;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  try {
    const savedTheme = localStorage.getItem("tick-theme");
    if (savedTheme === "light" || savedTheme === "dark") {
      document.documentElement.setAttribute("data-theme", savedTheme);
    }
  } catch (_) {}

  document.getElementById("theme-toggle").addEventListener("click", () => {
    const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    try { localStorage.setItem("tick-theme", next); } catch (_) {}
  });
  document.getElementById("print-btn").addEventListener("click", () => window.print());
  const isSf = report.target_platform === "snowflake";
  const platName = isSf ? "Snowflake" : "Databricks";
  document.getElementById("download-all-sql").addEventListener("click", () => {
    downloadBlob(
      isSf ? "unified_snowflake_views.sql" : "unified_databricks_views.sql",
      allSql,
      "text/sql;charset=utf-8"
    );
  });
  document.getElementById("hdr-sub").textContent =
    `Target: ${platName}${report.glossary_matching_enabled ? " · Glossary Semantic Matching: ON" : ""} · Comparing ${report.sources.length} plants/sources: ${report.sources.join(" · ")}`;

  const totalConflicts = report.tables.reduce((a, t) => a + t.conflict_count, 0);
  const totalSiteCols = report.tables.reduce((a, t) => a + t.site_specific_column_count, 0);
  const pkDivergentCount = report.tables.filter(t => t.pk_divergent).length;
  const missingTableCount = report.tables.filter(t => t.missing_in_sources.length > 0).length;
  const alignPct = report.total_unique_tables
    ? Math.round((report.fully_aligned_tables / report.total_unique_tables) * 100)
    : 100;

  const kpis = [
    { label: "Plants Compared", val: report.sources.length, hint: report.sources.slice(0, 3).join(", ") + (report.sources.length > 3 ? "…" : "") },
    { label: "Unique Tables", val: report.total_unique_tables, hint: `${report.fully_aligned_tables} fully aligned (${alignPct}%)` },
    { label: "Tables w/ Drift", val: report.tables_with_drift, hint: `${missingTableCount} missing in ≥1 plant` },
    { label: "Type Conflicts", val: totalConflicts, hint: isSf ? "Auto-widened in Snowflake SQL" : "Auto-widened in Spark SQL" },
    { label: "Site-Only Cols", val: totalSiteCols, hint: "CAST(NULL) padded in UNION" },
    { label: "PK Divergences", val: pkDivergentCount, hint: "Review Silver MERGE keys" },
  ];
  document.getElementById("kpi-grid").innerHTML = kpis.map(k =>
    `<div class="kpi"><div class="label">${esc(k.label)}</div><div class="val">${esc(k.val)}</div><div class="hint">${esc(k.hint)}</div></div>`
  ).join("");

  // Alignment Donut
  (function() {
    const segs = [
      { label: "Fully Aligned", value: report.fully_aligned_tables, color: "var(--green)" },
      { label: "Has Drift", value: report.tables_with_drift, color: "var(--amber)" },
    ];
    const total = report.total_unique_tables;
    const r = 46, cx = 60, cy = 60, c = 2 * Math.PI * r;
    let offset = 0;
    const arcs = total > 0 ? segs.filter(s => s.value > 0).map(s => {
      const dash = (s.value / total) * c;
      const out = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${s.color}" stroke-width="14" stroke-dasharray="${dash.toFixed(2)} ${(c - dash).toFixed(2)}" stroke-dashoffset="${(-offset).toFixed(2)}" transform="rotate(-90 ${cx} ${cy})" />`;
      offset += dash;
      return out;
    }).join("") : `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="var(--border)" stroke-width="14" />`;
    document.getElementById("chart-align-donut").innerHTML = `<div class="donut-wrap">
      <svg width="120" height="120" viewBox="0 0 120 120">
        <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="var(--bg)" stroke-width="14" />
        ${arcs}
        <text x="${cx}" y="${cy - 2}" text-anchor="middle" fill="var(--text)" font-size="17" font-weight="700" font-family="monospace">${alignPct}%</text>
        <text x="${cx}" y="${cy + 14}" text-anchor="middle" fill="var(--text-muted)" font-size="10">Aligned</text>
      </svg>
      <div class="legend-list">${segs.map(s => `
        <div class="legend-row">
          <span><span class="legend-swatch" style="background:${s.color}"></span>${esc(s.label)}</span>
          <span class="mono"><strong>${s.value}</strong></span>
        </div>`).join("")}</div>
    </div>`;
  })();

  // Drift Category Bars
  (function() {
    const items = [
      { label: "Tables Missing in ≥1 Plant", value: missingTableCount, color: "var(--red)" },
      { label: "Column Type Conflicts", value: totalConflicts, color: "var(--amber)" },
      { label: "Site-Specific Columns", value: totalSiteCols, color: "var(--orange)" },
      { label: "Primary Key Divergences", value: pkDivergentCount, color: "var(--purple)" },
    ];
    const maxV = Math.max(1, ...items.map(i => i.value));
    document.getElementById("chart-drift-bars").innerHTML = items.map(i => {
      const pct = i.value > 0 ? Math.max(4, Math.round((i.value / maxV) * 100)) : 0;
      return `<div style="margin-bottom:8px;">
        <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px;">
          <span>${esc(i.label)}</span><span class="mono"><strong>${i.value}</strong></span>
        </div>
        <div style="width:100%;height:8px;background:var(--bg);border:1px solid var(--border);border-radius:99px;overflow:hidden;">
          <div style="width:${pct}%;height:100%;background:${i.color};border-radius:99px;"></div>
        </div>
      </div>`;
    }).join("");
  })();

  let activeFilter = "all";
  let selectedTable = (report.tables.find(t => t.conflict_count > 0 || t.site_specific_column_count > 0 || t.missing_in_sources.length > 0 || t.pk_divergent) || report.tables[0])?.table_name || null;

  function plantCellState(tbl, src) {
    if (!tbl.present_in_sources.includes(src)) return { cls: "hc-missing", label: "MISSING", title: `Table missing in ${src}` };
    const casts = tbl.columns.filter(c => c.cast_required_sources.includes(src)).length;
    const missCols = tbl.columns.filter(c => c.missing_in_sources.includes(src)).length;
    const parts = [];
    if (casts > 0) parts.push(`${casts} cast(s)`);
    if (missCols > 0) parts.push(`${missCols} missing col(s)`);
    if (tbl.pk_divergent) parts.push("PK divergent");
    const tip = parts.length ? `${src}: ${parts.join(", ")}` : `${src}: fully aligned`;
    if (casts > 0 && missCols > 0) return { cls: "hc-conflict", label: `CAST(${casts}) -${missCols}C`, title: tip };
    if (casts > 0) return { cls: "hc-conflict", label: `CAST (${casts})`, title: tip };
    if (missCols > 0) return { cls: "hc-site", label: `-${missCols} COL`, title: tip };
    if (tbl.pk_divergent) return { cls: "hc-pk", label: "PK DIFF", title: tip };
    return { cls: "hc-aligned", label: "ALIGNED", title: tip };
  }

  function renderMatrix() {
    const q = (document.getElementById("tbl-search").value || "").toLowerCase().trim();
    const filtered = report.tables.filter(t => {
      const isDrifted = t.conflict_count > 0 || t.site_specific_column_count > 0 || t.missing_in_sources.length > 0 || t.pk_divergent;
      if (activeFilter === "drifted" && !isDrifted) return false;
      if (activeFilter === "conflicts" && t.conflict_count === 0) return false;
      if (activeFilter === "site_cols" && t.site_specific_column_count === 0) return false;
      if (activeFilter === "pk" && !t.pk_divergent) return false;
      if (activeFilter === "missing" && t.missing_in_sources.length === 0) return false;
      if (q) {
        const hitT = t.table_name.toLowerCase().includes(q);
        const hitC = t.columns.some(c => c.name.toLowerCase().includes(q) || c.sanitized_name.toLowerCase().includes(q));
        if (!hitT && !hitC) return false;
      }
      return true;
    });

    const headCols = report.sources.map(s => `<th>${esc(s)}</th>`).join("");
    document.getElementById("matrix-table").innerHTML = `
      <thead>
        <tr><th>Table</th>${headCols}<th>Conflicts</th><th>Site-Only</th><th>PK</th></tr>
      </thead>
      <tbody>
        ${filtered.length ? filtered.map(t => `
          <tr class="matrix-row ${t.table_name === selectedTable ? 'selected' : ''}" data-table="${esc(t.table_name)}">
            <td class="mono"><strong>${esc(t.table_name)}</strong>${t.matched_via_glossary ? ' <span class="badge b-purple" title="Matched across heterogeneous ERP/MES table names via glossary">GLOSSARY</span>' : ''}</td>
            ${report.sources.map(s => {
              const st = plantCellState(t, s);
              return `<td><span class="heat-cell ${st.cls}" title="${esc(st.title)}">${st.label}</span></td>`;
            }).join("")}
            <td class="mono">${t.conflict_count ? `<span class="badge b-amber">${t.conflict_count}</span>` : "0"}</td>
            <td class="mono">${t.site_specific_column_count ? `<span class="badge b-orange">${t.site_specific_column_count}</span>` : "0"}</td>
            <td>${t.pk_divergent ? `<span class="badge b-purple">DIVERGENT</span>` : `<span class="badge b-green">MATCH</span>`}</td>
          </tr>`).join("") : `<tr><td colspan="${report.sources.length + 4}" style="color:var(--text-muted)">No tables match the active filter.</td></tr>`}
      </tbody>`;

    document.querySelectorAll("tr.matrix-row").forEach(row => {
      row.addEventListener("click", () => {
        selectedTable = row.getAttribute("data-table");
        renderMatrix();
        renderInspector();
      });
    });
  }

  let showDriftedColsOnly = false;
  function renderInspector() {
    const host = document.getElementById("inspector-host");
    const tbl = report.tables.find(t => t.table_name === selectedTable);
    if (!tbl) { host.innerHTML = ""; return; }

    const cols = showDriftedColsOnly
      ? tbl.columns.filter(c => c.has_type_conflict || c.missing_in_sources.length > 0)
      : tbl.columns;

    const pkStrip = report.sources.map(s => {
      if (!tbl.present_in_sources.includes(s)) {
        return `<span class="badge b-red">${esc(s)}: TABLE MISSING</span>`;
      }
      const pk = (tbl.primary_keys_by_source[s] || []).join(", ") || "NONE";
      return `<span class="badge ${tbl.pk_divergent ? 'b-purple' : 'b-green'}">${esc(s)} PK: (${esc(pk)})</span>`;
    }).join("");

    const srcTblStrip = tbl.matched_via_glossary && tbl.source_table_names
      ? `<div style="margin-top:6px;">${Object.entries(tbl.source_table_names).map(([s, tn]) => `<span class="badge b-purple">${esc(s)} table: ${esc(tn)}</span>`).join("")}</div>`
      : "";

    host.innerHTML = `
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:10px;">
          <div>
            <h2 style="margin:0;font-family:var(--mono);font-size:17px;">Table Inspector: ${esc(tbl.table_name)}</h2>
            <div style="margin-top:6px;">${pkStrip}</div>
            ${srcTblStrip}
          </div>
          <label class="check-pill">
            <input type="checkbox" id="toggle-drifted-cols" ${showDriftedColsOnly ? "checked" : ""} /> Show drifted columns only
          </label>
        </div>

        <div style="overflow-x:auto;margin-bottom:16px;">
          <table>
            <thead>
              <tr>
                <th>Target Column</th>
                <th>Widened ${platName} Type</th>
                ${tbl.present_in_sources.map(s => `<th>${esc(s)}</th>`).join("")}
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              ${cols.length ? cols.map(c => {
                const tgtType = isSf ? (c.target_snowflake_type || c.target_spark_type) : c.target_spark_type;
                return `
                <tr>
                  <td class="mono"><strong>${esc(c.sanitized_name)}</strong>${c.name !== c.sanitized_name ? `<div style="font-size:11px;color:var(--text-muted)">src: ${esc(c.name)}</div>` : ""}</td>
                  <td><span class="badge b-accent">${esc(tgtType)}</span>${c.nullable_in_target ? '<span class="badge b-muted">NULL</span>' : '<span class="badge b-green">NN</span>'}</td>
                  ${tbl.present_in_sources.map(s => {
                    if (!c.present_in_sources.includes(s)) {
                      return `<td><span class="badge b-red">MISSING → NULL</span></td>`;
                    }
                    const needsCast = c.cast_required_sources.includes(s);
                    const rawColName = c.source_column_names ? c.source_column_names[s] : null;
                    const srcColHint = c.matched_via_glossary && rawColName ? `<div style="font-size:11px;color:var(--purple)">col: ${esc(rawColName)}</div>` : "";
                    return `<td class="mono">${esc(c.types_by_source[s] || "")}${needsCast ? ' <span class="badge b-amber">CAST</span>' : ""}${srcColHint}</td>`;
                  }).join("")}
                  <td>
                    ${c.matched_via_glossary ? '<span class="badge b-purple">GLOSSARY</span>' : ""}
                    ${c.has_type_conflict ? '<span class="badge b-amber">TYPE CONFLICT</span>' : ""}
                    ${c.missing_in_sources.length ? '<span class="badge b-orange">SITE-ONLY</span>' : ""}
                    ${(!c.has_type_conflict && !c.missing_in_sources.length) ? '<span class="badge b-green">ALIGNED</span>' : ""}
                  </td>
                </tr>`;
              }).join("") : `<tr><td colspan="${tbl.present_in_sources.length + 3}" style="color:var(--text-muted)">No drifted columns on this table.</td></tr>`}
            </tbody>
          </table>
        </div>

        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:8px;">
          <h2 style="margin:0;">Unified Multi-Plant ${platName} SQL</h2>
          <div style="display:flex;gap:8px;">
            <button class="btn" id="download-tbl-sql">Download SQL</button>
            <button class="btn btn-primary" id="copy-tbl-sql">Copy SQL</button>
          </div>
        </div>
        <pre class="code-block" id="tbl-sql-code">${esc(tbl.unified_sql)}</pre>
      </div>`;

    document.getElementById("toggle-drifted-cols").addEventListener("change", (e) => {
      showDriftedColsOnly = e.target.checked;
      renderInspector();
    });
    document.getElementById("copy-tbl-sql").addEventListener("click", async (e) => {
      try {
        await navigator.clipboard.writeText(tbl.unified_sql);
        e.target.textContent = "Copied!";
        setTimeout(() => e.target.textContent = "Copy SQL", 1500);
      } catch (_) {}
    });
    document.getElementById("download-tbl-sql").addEventListener("click", () => {
      downloadBlob(`unified_${tbl.table_name.replace(/[^a-zA-Z0-9_]+/g, "_")}.sql`, tbl.unified_sql, "text/sql;charset=utf-8");
    });
  }

  document.getElementById("tbl-search").addEventListener("input", renderMatrix);
  document.querySelectorAll(".pill-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      activeFilter = btn.dataset.filter;
      document.querySelectorAll(".pill-btn").forEach(b => b.classList.toggle("active", b === btn));
      renderMatrix();
    });
  });

  renderMatrix();
  renderInspector();
})();
</script>
</body>
</html>
"""


def render_harmonization_html(report: HarmonizationReport) -> str:
    """Render a standalone, offline-capable interactive HTML multi-plant harmonization dashboard."""
    all_sql_lines = [
        "-- Auto-generated by tick compare-sources",
        f"-- Harmonizing plants: {', '.join(report.sources)}",
        "",
        *[t.unified_sql for t in report.tables],
    ]
    payload = {
        "report": report.model_dump(mode="json"),
        "all_sql": "\n".join(all_sql_lines),
    }
    raw_json = json.dumps(payload).replace("</", "<\\/")
    title = " vs ".join(report.sources[:3]) + (f" (+{len(report.sources) - 3})" if len(report.sources) > 3 else "")
    return (
        HARMONIZE_HTML_TEMPLATE.replace("__TITLE__", html.escape(title))
        .replace("__PAYLOAD_JSON__", raw_json)
    )


def export_harmonization_html(report: HarmonizationReport, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_harmonization_html(report), encoding="utf-8")
    return path
