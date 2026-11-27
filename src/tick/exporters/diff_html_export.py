"""Self-contained interactive HTML report for snapshot schema & data drift diffs."""

from __future__ import annotations

import html
import json
from pathlib import Path

from tick.models import SnapshotDiff

DIFF_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>tick Diff — __TITLE__</title>
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
  .actions { display: flex; gap: 8px; }
  .btn {
    background: var(--bg-card); color: var(--text); border: 1px solid var(--border);
    padding: 6px 11px; border-radius: 7px; cursor: pointer; font-size: 12px; font-weight: 600;
  }
  .btn:hover { background: var(--bg-hover); }
  main { padding: 20px 24px; max-width: 1480px; margin: 0 auto; }

  .banner {
    border-radius: 10px; padding: 14px 18px; margin-bottom: 18px;
    display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px;
    border: 1px solid var(--border);
  }
  .banner.breaking { background: var(--red-soft); border-color: var(--red); }
  .banner.safe { background: var(--green-soft); border-color: var(--green); }
  .banner h2 { margin: 0; font-size: 16px; }
  .banner .meta { font-size: 12px; font-family: var(--mono); color: var(--text-muted); margin-top: 3px; }

  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; margin-bottom: 18px; }
  .kpi { background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
  .kpi .label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-muted); }
  .kpi .val { font-size: 22px; font-weight: 700; margin-top: 4px; font-family: var(--mono); }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 960px) { .grid-2 { grid-template-columns: 1fr; } }
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
  .b-muted { background: var(--bg-hover); color: var(--text-muted); }

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
  th { color: var(--text-muted); font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; background: var(--bg-elev); }
  tr:hover td { background: var(--bg-hover); }

  .drift-item {
    background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
    padding: 12px 14px; margin-bottom: 10px;
  }
  .drift-head { display: flex; justify-content: space-between; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }
  .cmp-bar-row { display: grid; grid-template-columns: 56px 1fr 78px; align-items: center; gap: 8px; margin-bottom: 4px; font-size: 11.5px; font-family: var(--mono); }
  .cmp-track { height: 9px; background: var(--bg-card); border: 1px solid var(--border); border-radius: 99px; overflow: hidden; }
  .cmp-fill { height: 100%; border-radius: 99px; }

  .list-stack { display: flex; flex-direction: column; gap: 6px; }
  .list-pill {
    padding: 7px 10px; border-radius: 7px; font-family: var(--mono); font-size: 12.5px;
    border: 1px solid var(--border); background: var(--bg);
  }
  .list-pill.added { border-left: 3px solid var(--green); }
  .list-pill.removed { border-left: 3px solid var(--red); }
  .list-pill.pk { border-left: 3px solid var(--amber); }
  .list-pill.pii { border-left: 3px solid var(--purple); }

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
      <h1>Schema &amp; Data Drift Report</h1>
      <div class="sub" id="hdr-sub"></div>
    </div>
  </div>
  <div class="actions">
    <button class="btn" id="print-btn">Print / PDF</button>
    <button class="btn" id="theme-toggle">Light / Dark</button>
  </div>
</header>

<main>
  <div id="status-banner"></div>
  <div class="kpi-grid" id="kpi-grid"></div>

  <div class="grid-2">
    <div class="card">
      <h2>Table &amp; Primary Key Changes</h2>
      <div id="table-changes-host"></div>
    </div>
    <div class="card">
      <h2>Newly Detected PII Alerts</h2>
      <div id="pii-alerts-host"></div>
    </div>
  </div>

  <div class="card">
    <h2>Column Schema Changes</h2>
    <div class="filters">
      <input id="col-search" class="search-input" type="text" placeholder="Search table or column..." />
      <button class="pill-btn active" data-filter="all">All Changes</button>
      <button class="pill-btn" data-filter="breaking">Breaking Only</button>
      <button class="pill-btn" data-filter="added">Added</button>
      <button class="pill-btn" data-filter="removed">Removed</button>
      <button class="pill-btn" data-filter="type_changed">Type Changed</button>
      <button class="pill-btn" data-filter="nullability_changed">Nullability</button>
    </div>
    <div style="overflow-x:auto"><table id="col-changes-table"></table></div>
  </div>

  <div class="card">
    <h2>Metric &amp; Data Quality Drifts</h2>
    <div id="metric-drifts-host"></div>
  </div>
</main>

<script id="tick-diff-data" type="application/json">__PAYLOAD_JSON__</script>
<script>
(function() {
  const diff = JSON.parse(document.getElementById("tick-diff-data").textContent);
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  const sevBadge = (sev) => {
    const cls = sev === "high" ? "b-red" : sev === "medium" ? "b-amber" : "b-muted";
    return `<span class="badge ${cls}">${esc(String(sev || "").toUpperCase())}</span>`;
  };

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

  document.getElementById("hdr-sub").textContent =
    `${diff.before_database} (${diff.before_discovered_at}) → ${diff.after_database} (${diff.after_discovered_at})`;

  document.getElementById("status-banner").innerHTML = `
    <div class="banner ${diff.has_breaking_changes ? 'breaking' : 'safe'}">
      <div>
        <h2>${diff.has_breaking_changes ? `Breaking Changes Detected (${diff.breaking_changes_count})` : "No Breaking Schema Changes Detected"}</h2>
        <div class="meta">Before: ${esc(diff.before_database)} @ ${esc(diff.before_discovered_at)} · After: ${esc(diff.after_database)} @ ${esc(diff.after_discovered_at)}</div>
      </div>
      <span class="badge ${diff.has_breaking_changes ? 'b-red' : 'b-green'}" style="font-size:12px;padding:5px 12px;">
        ${diff.has_breaking_changes ? "CI GATE: FAIL" : "CI GATE: PASS"}
      </span>
    </div>`;

  const kpis = [
    { label: "Breaking Changes", val: diff.breaking_changes_count },
    { label: "Added Tables", val: diff.added_tables.length },
    { label: "Removed Tables", val: diff.removed_tables.length },
    { label: "PK Changes", val: diff.primary_key_changes.length },
    { label: "Column Changes", val: diff.column_changes.length },
    { label: "New PII Cols", val: diff.new_pii_columns.length },
    { label: "Metric Drifts", val: diff.metric_drifts.length },
  ];
  document.getElementById("kpi-grid").innerHTML = kpis.map(k =>
    `<div class="kpi"><div class="label">${esc(k.label)}</div><div class="val">${esc(k.val)}</div></div>`
  ).join("");

  // Table & PK Changes
  const tblItems = [];
  diff.removed_tables.forEach(t => tblItems.push(`<div class="list-pill removed"><span class="badge b-red">REMOVED · BREAKING</span> ${esc(t)}</div>`));
  diff.primary_key_changes.forEach(p => tblItems.push(`<div class="list-pill pk"><span class="badge b-amber">PK CHANGED · BREAKING</span> ${esc(p)}</div>`));
  diff.added_tables.forEach(t => tblItems.push(`<div class="list-pill added"><span class="badge b-green">ADDED</span> ${esc(t)}</div>`));
  document.getElementById("table-changes-host").innerHTML = tblItems.length
    ? `<div class="list-stack">${tblItems.join("")}</div>`
    : `<div style="color:var(--text-muted)">No table additions, removals, or primary key changes.</div>`;

  // New PII Alerts
  document.getElementById("pii-alerts-host").innerHTML = diff.new_pii_columns.length
    ? `<div class="list-stack">${diff.new_pii_columns.map(p => `<div class="list-pill pii"><span class="badge b-purple">NEW PII</span> ${esc(p)}</div>`).join("")}</div>`
    : `<div style="color:var(--text-muted)">No newly surfaced PII columns detected.</div>`;

  // Filterable Column Changes
  let activeFilter = "all";
  function renderColumnChanges() {
    const q = (document.getElementById("col-search").value || "").toLowerCase().trim();
    const items = diff.column_changes.filter(c => {
      if (activeFilter === "breaking" && !c.breaking) return false;
      if (["added", "removed", "type_changed", "nullability_changed"].includes(activeFilter) && c.change_type !== activeFilter) return false;
      if (q && !`${c.table} ${c.column} ${c.change_type} ${c.before || ""} ${c.after || ""}`.toLowerCase().includes(q)) return false;
      return true;
    });
    document.getElementById("col-changes-table").innerHTML = `
      <thead><tr><th>Table</th><th>Column</th><th>Change</th><th>Before</th><th>After</th><th>Impact</th></tr></thead>
      <tbody>
        ${items.length ? items.map(c => {
          const chCls = c.change_type === "added" ? "b-green" : c.change_type === "removed" ? "b-red" : "b-amber";
          return `<tr>
            <td class="mono">${esc(c.table)}</td>
            <td class="mono"><strong>${esc(c.column)}</strong></td>
            <td><span class="badge ${chCls}">${esc(c.change_type)}</span></td>
            <td class="mono">${c.before ? `<span class="badge b-muted">${esc(c.before)}</span>` : "—"}</td>
            <td class="mono">${c.after ? `<span class="badge b-accent">${esc(c.after)}</span>` : "—"}</td>
            <td>${c.breaking ? '<span class="badge b-red">BREAKING</span>' : '<span class="badge b-green">SAFE</span>'}</td>
          </tr>`;
        }).join("") : `<tr><td colspan="6" style="color:var(--text-muted)">No column changes match the active filter.</td></tr>`}
      </tbody>`;
  }

  document.getElementById("col-search").addEventListener("input", renderColumnChanges);
  document.querySelectorAll(".pill-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      activeFilter = btn.dataset.filter;
      document.querySelectorAll(".pill-btn").forEach(b => b.classList.toggle("active", b === btn));
      renderColumnChanges();
    });
  });
  renderColumnChanges();

  // Visual Metric Drifts (Before vs After SVG/HTML comparative bars)
  const driftHost = document.getElementById("metric-drifts-host");
  if (!diff.metric_drifts.length) {
    driftHost.innerHTML = `<div style="color:var(--text-muted)">No significant row-count or null-rate metric drifts detected.</div>`;
  } else {
    driftHost.innerHTML = diff.metric_drifts.map(m => {
      const isNull = m.metric === "null_rate";
      const maxScale = isNull ? 100 : Math.max(1, Number(m.before) || 0, Number(m.after) || 0);
      const bPct = Math.min(100, Math.max(0, ((Number(m.before) || 0) / maxScale) * 100));
      const aPct = Math.min(100, Math.max(0, ((Number(m.after) || 0) / maxScale) * 100));
      const fmtVal = (v) => isNull ? `${Number(v).toFixed(1)}%` : Number(v).toLocaleString();
      const deltaSign = Number(m.delta) > 0 ? "+" : "";
      const pctSuffix = (!isNull && m.pct_change != null) ? ` (${Number(m.pct_change) > 0 ? "+" : ""}${Number(m.pct_change).toFixed(1)}%)` : "";
      const deltaLabel = isNull
        ? `${deltaSign}${Number(m.delta).toFixed(1)}pp`
        : `${deltaSign}${Number(m.delta).toLocaleString()} rows${pctSuffix}`;
      const afterColor = m.severity === "high" ? "var(--red)" : "var(--amber)";
      const afterValLabel = (!isNull && Number(m.after) === 0) ? "0 (EMPTY)" : fmtVal(m.after);
      return `<div class="drift-item">
        <div class="drift-head">
          <div>
            ${sevBadge(m.severity)}
            <span class="badge b-accent">${esc(m.metric)}</span>
            <span class="mono"><strong>${esc(m.table)}</strong>${m.column ? '.' + esc(m.column) : ''}</span>
          </div>
          <span class="badge ${m.severity === 'high' ? 'b-red' : 'b-amber'}">Delta: ${esc(deltaLabel)}</span>
        </div>
        <div class="cmp-bar-row">
          <span style="color:var(--text-muted)">Before</span>
          <div class="cmp-track"><div class="cmp-fill" style="width:${bPct.toFixed(1)}%;background:var(--accent);"></div></div>
          <span style="text-align:right;">${esc(fmtVal(m.before))}</span>
        </div>
        <div class="cmp-bar-row">
          <span style="color:var(--text-muted)">After</span>
          <div class="cmp-track"><div class="cmp-fill" style="width:${aPct.toFixed(1)}%;background:${afterColor};"></div></div>
          <span style="text-align:right;font-weight:700;">${esc(afterValLabel)}</span>
        </div>
      </div>`;
    }).join("");
  }
})();
</script>
</body>
</html>
"""


def render_diff_html(diff: SnapshotDiff) -> str:
    """Render a standalone, offline-capable interactive HTML schema & data drift report."""
    raw_json = json.dumps(diff.model_dump(mode="json")).replace("</", "<\\/")
    title = f"{diff.before_database} vs {diff.after_database}"
    return (
        DIFF_HTML_TEMPLATE.replace("__TITLE__", html.escape(title))
        .replace("__PAYLOAD_JSON__", raw_json)
    )


def export_diff_html(diff: SnapshotDiff, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_diff_html(diff), encoding="utf-8")
    return path
