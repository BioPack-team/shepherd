// Shepherd Monitor — history view. Pulls 30-day archive from Postgres-backed
// endpoints; refresh is manual (no auto-poll). All charts share the selected
// time window, set via the range bar at the top.

(() => {
  const COLORS = [
    "#58a6ff", "#3fb950", "#d29922", "#f85149", "#a371f7",
    "#ff7b72", "#79c0ff", "#56d364", "#e3b341", "#bc8cff",
    "#ffa657", "#7ee787", "#ffd33d",
  ];

  let currentWindow = { since: null, until: null, label: "24h" };
  const charts = {};

  // ---- helpers --------------------------------------------------------

  function fmtBytes(n) {
    if (!n || n < 0) return "0";
    const u = ["B", "KB", "MB", "GB", "TB"];
    let i = 0; let v = n;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v < 10 ? 1 : 0)} ${u[i]}`;
  }

  function fmtCount(n) {
    if (n == null) return "-";
    if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(1) + "k";
    return String(n);
  }

  function fmtDuration(ms) {
    if (ms == null) return "-";
    if (ms < 1000) return `${ms.toFixed(0)}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
  }

  function rangeFromPreset(key) {
    const now = Date.now() / 1000;
    const span = {
      "1h": 3600,
      "6h": 6 * 3600,
      "24h": 24 * 3600,
      "7d": 7 * 86400,
      "30d": 30 * 86400,
    }[key] || 24 * 3600;
    return { since: now - span, until: now, label: key };
  }

  function setWindow(win) {
    currentWindow = win;
    const fromStr = new Date(win.since * 1000).toLocaleString();
    const toStr = new Date(win.until * 1000).toLocaleString();
    document.getElementById("range-label").textContent = `${fromStr} → ${toStr}`;
    refresh();
  }

  // ---- charts ---------------------------------------------------------

  function timeAxisOpts() {
    return {
      x: {
        type: "linear",
        ticks: {
          color: "#8b949e",
          callback: v => new Date(v * 1000).toLocaleString([], {
            month: "short", day: "numeric",
            hour: "numeric", minute: "2-digit",
          }),
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: 8,
        },
      },
      y: { beginAtZero: true, ticks: { color: "#8b949e" } },
    };
  }

  function mkChart(canvasId, type, datasets, extraOpts) {
    const ctx = document.getElementById(canvasId).getContext("2d");
    return new Chart(ctx, {
      type,
      data: { datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        parsing: false,
        plugins: {
          legend: { labels: { color: "#8b949e", boxWidth: 10 } },
        },
        scales: timeAxisOpts(),
        ...(extraOpts || {}),
      },
    });
  }

  function replaceDatasets(chart, datasets) {
    chart.data.datasets = datasets;
    chart.update("none");
  }

  function seriesToDataset(label, points, color, opts = {}) {
    return {
      label,
      data: points.map(p => ({ x: p[0], y: p[1] })),
      borderColor: color,
      backgroundColor: opts.fill ? color + "33" : color,
      borderWidth: opts.borderWidth ?? 1.5,
      pointRadius: 0,
      fill: opts.fill ?? false,
      tension: 0.2,
      stepped: opts.stepped ?? false,
    };
  }

  // ---- data loaders ---------------------------------------------------

  async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${url} -> ${res.status}`);
    return await res.json();
  }

  function qs(params) {
    return Object.entries(params)
      .filter(([_, v]) => v !== undefined && v !== null && v !== "")
      .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
      .join("&");
  }

  async function loadSummary() {
    const { since, until } = currentWindow;
    const s = await fetchJSON(`/api/historical/summary?${qs({ since, until })}`);
    const grid = document.getElementById("summary");
    const cards = [
      { label: "Queries started", value: fmtCount(s.queries_started) },
      { label: "Crashes", value: fmtCount(s.crashes), bad: s.crashes > 0 },
      { label: "Scale events", value: fmtCount(s.scale_events) },
      { label: "Alerts", value: fmtCount(s.alert_count), bad: s.alert_count > 0 },
    ];
    grid.innerHTML = cards.map(c => `
      <div class="summary-card ${c.bad ? "bad" : ""}">
        <div class="label">${c.label}</div>
        <div class="value">${c.value}</div>
      </div>
    `).join("");
  }

  async function loadThroughput() {
    // Throughput approximation: per-ARA queries_last_1h doesn't tile across
    // long windows, so we use pg:state:* aggregated. Cleaner is summing
    // per-ara queries; for now, just plot total state counts as a proxy.
    const { since, until } = currentWindow;
    // We'll instead pull a clean signal: workers_alive total is uncorrelated.
    // Use pg:queries_last_1h as a proxy for hourly throughput. Better: use
    // the rate of new shepherd_brain rows -- not in metrics. For now, plot
    // queries_last_1h across the window.
    const r = await fetchJSON(
      `/api/historical/metrics?${qs({ metric: "pg:queries_last_1h", since, until })}`
    );
    const series = r.series["pg:queries_last_1h"] || [];
    if (!charts.throughput) {
      charts.throughput = mkChart("throughput-chart", "line", []);
    }
    replaceDatasets(charts.throughput, [
      seriesToDataset("Queries (last 1h, sampled)", series, COLORS[0], { fill: true }),
    ]);
  }

  async function loadFleet() {
    const { since, until } = currentWindow;
    const r = await fetchJSON(
      `/api/historical/metrics_by_prefix?${qs({ prefix: "workers_alive:", since, until })}`
    );
    const datasets = [];
    const names = Object.keys(r.series).sort();
    names.forEach((name, i) => {
      const short = name.replace(/^workers_alive:/, "");
      datasets.push(seriesToDataset(short, r.series[name], COLORS[i % COLORS.length], { stepped: true }));
    });
    if (!charts.fleet) {
      charts.fleet = mkChart("fleet-chart", "line", []);
    }
    replaceDatasets(charts.fleet, datasets);
  }

  async function loadBacklog() {
    const { since, until } = currentWindow;
    const r = await fetchJSON(
      `/api/historical/metrics_by_prefix?${qs({ prefix: "xlen:", since, until })}`
    );
    // Only show streams with some non-zero traffic to keep the legend useful.
    const datasets = [];
    const entries = Object.entries(r.series)
      .filter(([_, pts]) => pts.some(p => p[1] > 0))
      .sort();
    entries.forEach(([name, pts], i) => {
      const short = name.replace(/^xlen:/, "");
      datasets.push(seriesToDataset(short, pts, COLORS[i % COLORS.length]));
    });
    if (!charts.backlog) {
      charts.backlog = mkChart("backlog-chart", "line", []);
    }
    replaceDatasets(charts.backlog, datasets);
  }

  async function loadLatency() {
    const { since, until } = currentWindow;
    const r = await fetchJSON(
      `/api/historical/latency?${qs({ since, until })}`
    );
    const grid = document.getElementById("latency-grid");
    const streams = Object.keys(r.series).sort();
    if (streams.length === 0) {
      grid.innerHTML = '<div class="empty">No task latency data in this window.</div>';
      return;
    }
    // Render small-multiples: one card per stream with p50/p95/p99 lines.
    grid.innerHTML = streams.map(s => `
      <div class="latency-card">
        <div class="latency-header">
          <div class="name">${s}</div>
          <div class="latency-meta" id="latency-meta-${cssEscape(s)}"></div>
        </div>
        <div class="latency-chart-wrap"><canvas id="latency-${cssEscape(s)}"></canvas></div>
      </div>
    `).join("");
    for (const stream of streams) {
      const points = r.series[stream];
      const safe = cssEscape(stream);
      const ctx = document.getElementById(`latency-${safe}`).getContext("2d");
      // destroy any previous chart on this canvas
      if (charts[`latency:${stream}`]) charts[`latency:${stream}`].destroy();
      const ds = [
        seriesToDataset("p50", points.map(p => [p.ts, p.p50_ms]), COLORS[1]),
        seriesToDataset("p95", points.map(p => [p.ts, p.p95_ms]), COLORS[2]),
        seriesToDataset("p99", points.map(p => [p.ts, p.p99_ms]), COLORS[3]),
      ];
      charts[`latency:${stream}`] = new Chart(ctx, {
        type: "line",
        data: { datasets: ds },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          parsing: false,
          plugins: { legend: { labels: { color: "#8b949e", boxWidth: 8, font: { size: 10 } } } },
          scales: timeAxisOpts(),
        },
      });
      const last = points[points.length - 1] || {};
      const totalTasks = points.reduce((a, p) => a + (p.count || 0), 0);
      document.getElementById(`latency-meta-${safe}`).innerHTML = `
        <span>${fmtCount(totalTasks)} tasks</span>
        <span>p50 ${fmtDuration(last.p50_ms)}</span>
        <span>p95 ${fmtDuration(last.p95_ms)}</span>
        <span>p99 ${fmtDuration(last.p99_ms)}</span>
      `;
    }
  }

  function cssEscape(s) {
    return s.replace(/[^a-zA-Z0-9]/g, "_");
  }

  async function loadHeatmap() {
    const { since, until } = currentWindow;
    const [bl, cap] = await Promise.all([
      fetchJSON(`/api/historical/metrics_by_prefix?${qs({ prefix: "backlog:", since, until })}`),
      fetchJSON(`/api/historical/metrics_by_prefix?${qs({ prefix: "capacity:", since, until })}`),
    ]);
    // Find the time-buckets used (assume backlog and capacity share them).
    const streams = new Set();
    Object.keys(bl.series).forEach(k => streams.add(k.replace("backlog:", "")));
    Object.keys(cap.series).forEach(k => streams.add(k.replace("capacity:", "")));
    const streamList = [...streams].sort();
    if (streamList.length === 0) {
      document.getElementById("heatmap").innerHTML = '<div class="empty">No fleet history in this window.</div>';
      return;
    }
    // Build per-stream array of utilization points.
    const utilByStream = {};
    let allTs = new Set();
    streamList.forEach(s => {
      const bps = bl.series["backlog:" + s] || [];
      const cps = cap.series["capacity:" + s] || [];
      const capMap = new Map(cps.map(p => [Math.round(p[0]), p[1]]));
      utilByStream[s] = bps.map(p => {
        const tsKey = Math.round(p[0]);
        const c = capMap.get(tsKey);
        const util = c && c > 0 ? Math.min(2, p[1] / c) : (p[1] > 0 ? 1 : 0);
        allTs.add(tsKey);
        return { ts: tsKey, util };
      });
    });
    const sortedTs = [...allTs].sort((a, b) => a - b);
    // Render as a simple HTML grid: rows = streams, cells = colored squares.
    const cellSize = Math.max(2, Math.floor(900 / Math.max(1, sortedTs.length)));
    let html = '<table class="heatmap-table"><tbody>';
    for (const s of streamList) {
      const cellsByTs = new Map(utilByStream[s].map(p => [p.ts, p.util]));
      html += `<tr><td class="hm-label">${s}</td>`;
      for (const ts of sortedTs) {
        const u = cellsByTs.get(ts);
        const color = utilColor(u);
        const tip = u == null ? "n/a" : `util=${(u * 100).toFixed(0)}%`;
        html += `<td class="hm-cell" style="width:${cellSize}px;background:${color}" title="${new Date(ts * 1000).toLocaleString()} ${tip}"></td>`;
      }
      html += "</tr>";
    }
    html += "</tbody></table>";
    document.getElementById("heatmap").innerHTML = html;
  }

  function utilColor(u) {
    if (u == null) return "transparent";
    // 0 -> dark; 0.3 green; 1 yellow; >1.5 red
    if (u < 0.05) return "#1a2129";
    if (u < 0.3) return "#1f5a2d";
    if (u < 0.7) return "#3fb950";
    if (u < 1.0) return "#d29922";
    return "#f85149";
  }

  async function loadInfra() {
    const { since, until } = currentWindow;
    const r = await fetchJSON(
      `/api/historical/metrics?${qs({
        metric: "redis:used_memory_bytes,pg:connection_count,pg:db_size_bytes,redis:ops_per_sec",
        since, until,
      })}`
    );
    if (!charts.redisMem) charts.redisMem = mkChart("redis-mem-chart", "line", []);
    replaceDatasets(charts.redisMem, [
      seriesToDataset("Redis memory", r.series["redis:used_memory_bytes"] || [], COLORS[0], { fill: true }),
      seriesToDataset("Postgres DB size", r.series["pg:db_size_bytes"] || [], COLORS[4], { fill: false }),
    ]);
    charts.redisMem.options.scales.y.ticks.callback = v => fmtBytes(v);
    charts.redisMem.update("none");
    if (!charts.pgConn) charts.pgConn = mkChart("pg-conn-chart", "line", []);
    replaceDatasets(charts.pgConn, [
      seriesToDataset("PG connections", r.series["pg:connection_count"] || [], COLORS[1]),
      seriesToDataset("Redis ops/s", r.series["redis:ops_per_sec"] || [], COLORS[2]),
    ]);
  }

  async function loadEvents() {
    const { since, until } = currentWindow;
    const r = await fetchJSON(
      `/api/historical/events?${qs({ since, until, limit: 500 })}`
    );
    renderTimeline(r.events || []);
    renderIncidentsTable((r.events || []).filter(e => e.severity && e.severity !== "info"));
  }

  function renderTimeline(events) {
    const el = document.getElementById("events-timeline");
    if (!events.length) {
      el.innerHTML = '<div class="empty">No events in this window.</div>';
      return;
    }
    const span = Math.max(60, currentWindow.until - currentWindow.since);
    const dots = events.map(ev => {
      const x = ((ev.ts - currentWindow.since) / span) * 100;
      let cls = "info";
      if (ev.severity === "critical") cls = "critical";
      else if (ev.severity === "warning") cls = "warning";
      else if (ev.type === "scale_up") cls = "scale_up";
      else if (ev.type === "scale_down") cls = "scale_down";
      const title = `${new Date(ev.ts * 1000).toLocaleString()} · ${ev.type}${ev.worker ? " · " + ev.worker : ""}${ev.detail ? " · " + ev.detail : ""}`;
      return `<div class="dot ${cls}" style="left:${x.toFixed(2)}%" title="${escapeHtml(title)}"></div>`;
    }).join("");
    el.innerHTML = `<div class="timeline-bar">${dots}</div>`;
  }

  function renderIncidentsTable(events) {
    const tbody = document.querySelector("#incidents-table tbody");
    if (!events.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="empty">No incidents in this window.</td></tr>';
      return;
    }
    tbody.innerHTML = events.map(ev => `
      <tr>
        <td>${new Date(ev.ts * 1000).toLocaleString()}</td>
        <td>${ev.type}</td>
        <td class="sev ${ev.severity || "info"}">${ev.severity || "info"}</td>
        <td>${ev.worker || "-"}</td>
        <td>${escapeHtml(ev.detail || "")}</td>
      </tr>
    `).join("");
  }

  function escapeHtml(s) {
    return String(s).replace(/[<>&"]/g, c => ({"<": "&lt;", ">": "&gt;", "&": "&amp;", '"': "&quot;"}[c]));
  }

  async function refresh() {
    try {
      await Promise.all([
        loadSummary(),
        loadThroughput(),
        loadFleet(),
        loadBacklog(),
        loadLatency(),
        loadHeatmap(),
        loadInfra(),
        loadEvents(),
      ]);
    } catch (e) {
      console.error("Refresh failed", e);
    }
  }

  // ---- wiring ---------------------------------------------------------

  document.querySelectorAll(".range-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".range-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      setWindow(rangeFromPreset(btn.dataset.range));
    });
  });

  document.getElementById("custom-apply").addEventListener("click", () => {
    const f = document.getElementById("custom-from").value;
    const t = document.getElementById("custom-to").value;
    if (!f || !t) return;
    const since = new Date(f).getTime() / 1000;
    const until = new Date(t).getTime() / 1000;
    if (!(since < until)) return;
    document.querySelectorAll(".range-btn").forEach(b => b.classList.remove("active"));
    setWindow({ since, until, label: "custom" });
  });

  document.getElementById("refresh-btn").addEventListener("click", refresh);

  setWindow(rangeFromPreset("24h"));
})();
