// Shepherd Monitor dashboard client.
// Connects via websocket, falls back to /api/snapshot polling if the socket
// drops. Renders worker cards, queue table, query summary, infra panel, and
// the alerts feed. Two Chart.js instances track XLEN per stream and per-ARA
// 24h volume; both are updated in place on each tick.

(() => {
  const connectionEl = document.getElementById("connection");
  const lastUpdateEl = document.getElementById("last-update");
  const workersEl = document.getElementById("workers");
  const streamsBody = document.querySelector("#streams-table tbody");
  const querySummary = document.getElementById("query-summary");
  const infraEl = document.getElementById("infra");
  const alertsEl = document.getElementById("alerts");

  let xlenChart, araChart;
  let xlenHistory = {}; // stream -> [{x, y}, ...]
  const HISTORY_POINTS = 60; // rolling window in the chart

  function fmt(n) {
    if (n === null || n === undefined) return "-";
    if (typeof n !== "number") return n;
    if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(1) + "k";
    return String(n);
  }

  function fmtAge(seconds) {
    if (!seconds && seconds !== 0) return "-";
    if (seconds < 60) return `${Math.round(seconds)}s`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
    return `${Math.round(seconds / 3600)}h`;
  }

  function setConnection(online) {
    connectionEl.classList.toggle("online", online);
    connectionEl.classList.toggle("offline", !online);
    connectionEl.textContent = online ? "live" : "disconnected";
  }

  function ensureCharts() {
    if (!xlenChart) {
      const ctx = document.getElementById("xlen-chart").getContext("2d");
      xlenChart = new Chart(ctx, {
        type: "line",
        data: { datasets: [] },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          parsing: false,
          plugins: {
            legend: { labels: { color: "#8b949e", boxWidth: 10 } },
            title: { display: true, text: "Queue depth (XLEN)", color: "#e6edf3" },
          },
          scales: {
            x: { type: "linear", ticks: { color: "#8b949e", callback: v => new Date(v * 1000).toLocaleTimeString() } },
            y: { beginAtZero: true, ticks: { color: "#8b949e" } },
          },
        },
      });
    }
    if (!araChart) {
      const ctx = document.getElementById("ara-chart").getContext("2d");
      araChart = new Chart(ctx, {
        type: "bar",
        data: { labels: [], datasets: [{ label: "Requests (24h)", data: [], backgroundColor: "#58a6ff" }] },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: "#8b949e" } },
            y: { beginAtZero: true, ticks: { color: "#8b949e" } },
          },
        },
      });
    }
  }

  const COLORS = [
    "#58a6ff", "#3fb950", "#d29922", "#f85149", "#a371f7",
    "#ff7b72", "#79c0ff", "#56d364", "#e3b341", "#bc8cff",
  ];

  function updateWorkers(workers) {
    const names = Object.keys(workers).sort();
    workersEl.innerHTML = "";
    if (names.length === 0) {
      workersEl.innerHTML = '<div class="card"><div class="name">No workers reporting yet</div></div>';
      return;
    }
    for (const name of names) {
      const w = workers[name];
      const card = document.createElement("div");
      card.className = "card";
      const aliveClass = w.alive > 0 ? "alive" : "down";
      const staleClass = w.stale > 0 ? "stale" : "";
      card.innerHTML = `
        <div class="name">${name}</div>
        <div class="counts">
          <span><span class="label">Alive</span><span class="value ${aliveClass}">${w.alive}</span></span>
          <span><span class="label">Stale</span><span class="value ${staleClass}">${w.stale}</span></span>
          <span><span class="label">Capacity</span><span class="value">${fmt(w.task_limit_total)}</span></span>
        </div>
      `;
      workersEl.appendChild(card);
    }
  }

  function updateStreams(streams, ts) {
    const names = Object.keys(streams).sort();
    streamsBody.innerHTML = "";
    for (const name of names) {
      const s = streams[name];
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${name}</td>
        <td class="num">${fmt(s.xlen)}</td>
        <td class="num">${fmt(s.pending)}</td>
        <td class="num">${fmt(s.consumer_count)}</td>
        <td class="num">${(s.max_idle_ms / 1000).toFixed(1)}</td>
      `;
      streamsBody.appendChild(tr);
    }

    // Update rolling xlen chart -- only graph streams with non-zero traffic
    // to avoid a wall of flat lines.
    for (const name of names) {
      if (!xlenHistory[name]) xlenHistory[name] = [];
      xlenHistory[name].push({ x: ts, y: streams[name].xlen });
      if (xlenHistory[name].length > HISTORY_POINTS) {
        xlenHistory[name].shift();
      }
    }
    const interesting = names.filter(n => xlenHistory[n].some(p => p.y > 0));
    xlenChart.data.datasets = interesting.map((n, i) => ({
      label: n,
      data: xlenHistory[n].slice(),
      borderColor: COLORS[i % COLORS.length],
      backgroundColor: COLORS[i % COLORS.length],
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.2,
    }));
    xlenChart.update("none");
  }

  function updateQuerySummary(pg) {
    const state = pg.state_counts || {};
    const summary = [
      { label: "Queued", value: state.QUEUED || 0 },
      { label: "Completed", value: state.COMPLETED || 0 },
      { label: "Last 1h", value: pg.queries_last_1h || 0 },
      { label: "Last 24h", value: pg.queries_last_24h || 0 },
      { label: "Callbacks pending", value: pg.callbacks_pending || 0 },
      { label: "Oldest cb age", value: fmtAge(pg.oldest_callback_age_sec) },
    ];
    querySummary.innerHTML = summary
      .map(s => `<div class="kv"><span class="label">${s.label}</span><span class="value">${s.value}</span></div>`)
      .join("");
  }

  function updateInfra(pg, redis) {
    const rows = [
      { label: "PG connections", value: pg.connection_count ?? "-" },
      { label: "Redis memory", value: redis.used_memory_human || "-" },
      { label: "Redis clients", value: redis.connected_clients ?? "-" },
      { label: "Redis ops/s", value: redis.instantaneous_ops_per_sec ?? "-" },
    ];
    infraEl.innerHTML = rows
      .map(r => `<div class="kv"><span class="label">${r.label}</span><span class="value">${r.value}</span></div>`)
      .join("");
  }

  function updateAraChart(pg) {
    const perAra = pg.per_ara_24h || {};
    const labels = Object.keys(perAra).sort();
    araChart.data.labels = labels;
    araChart.data.datasets[0].data = labels.map(l => perAra[l]);
    araChart.update("none");
  }

  async function loadAlerts() {
    try {
      const res = await fetch("/api/alerts?limit=50");
      const body = await res.json();
      renderAlerts(body.alerts || []);
    } catch (e) {
      // ignore -- websocket will keep us live
    }
  }

  function renderAlerts(items) {
    if (!items.length) {
      alertsEl.innerHTML = '<div class="empty">No alerts.</div>';
      return;
    }
    alertsEl.innerHTML = items
      .map(a => {
        const when = new Date(a.ts * 1000).toLocaleString();
        const sev = (a.severity || "warning").toLowerCase();
        return `<li>
          <span class="sev ${sev}">${sev}</span>
          <span class="rule">${a.rule}</span>
          <span>${a.message || a.detail || ""}</span>
          <span class="when">${when}</span>
        </li>`;
      })
      .join("");
  }

  function applySnapshot(snap) {
    ensureCharts();
    lastUpdateEl.textContent = `updated ${new Date(snap.ts * 1000).toLocaleTimeString()}`;
    updateWorkers(snap.workers || {});
    updateStreams(snap.streams || {}, snap.ts);
    updateQuerySummary(snap.postgres || {});
    updateInfra(snap.postgres || {}, snap.redis || {});
    updateAraChart(snap.postgres || {});
    if (snap.new_alerts && snap.new_alerts.length) {
      loadAlerts();
    }
  }

  // ---- transport ----------------------------------------------------------

  let ws;
  let pollTimer;

  function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(`${proto}://${location.host}/ws`);
    ws.onopen = () => {
      setConnection(true);
      if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    };
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === "snapshot") applySnapshot(msg.data);
      } catch (e) {
        console.error("Bad ws payload", e);
      }
    };
    ws.onclose = () => {
      setConnection(false);
      // Fall back to polling and keep retrying the socket.
      if (!pollTimer) {
        pollTimer = setInterval(async () => {
          try {
            const r = await fetch("/api/snapshot");
            applySnapshot(await r.json());
          } catch (e) { /* swallow */ }
        }, 5000);
      }
      setTimeout(connect, 3000);
    };
    ws.onerror = () => { /* onclose will handle reconnect */ };
  }

  // Initial fetch so the UI is populated before the socket opens.
  fetch("/api/snapshot")
    .then(r => r.json())
    .then(applySnapshot)
    .catch(() => {});
  loadAlerts();
  setInterval(loadAlerts, 30000);
  connect();
})();
