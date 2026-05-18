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
  const eventsEl = document.getElementById("events");

  // Client-side rolling buffer of scaling events. The server sends fresh deltas
  // on each tick; we accumulate them so the user can see recent history.
  const eventBuffer = [];
  const EVENT_BUFFER_LIMIT = 100;

  let xlenChart, araChart;
  let xlenHistory = {}; // stream -> [{x, y}, ...]
  const HISTORY_POINTS = 60; // rolling window in the chart
  // Tracks chart legend items the user clicked to hide. Datasets are rebuilt
  // on every snapshot tick, which wipes Chart.js's internal visibility state,
  // so we mirror it here (keyed by series label) and re-apply on each rebuild.
  const xlenHiddenSeries = new Set();

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
            legend: {
              labels: { color: "#8b949e", boxWidth: 10 },
              onClick: (e, legendItem, legend) => {
                Chart.defaults.plugins.legend.onClick(e, legendItem, legend);
                const label = legendItem.text;
                if (legend.chart.isDatasetVisible(legendItem.datasetIndex)) {
                  xlenHiddenSeries.delete(label);
                } else {
                  xlenHiddenSeries.add(label);
                }
              },
            },
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

  function utilizationClass(ratio) {
    if (ratio < 0.3) return "low";
    if (ratio < 0.8) return "mid";
    return "high";
  }

  function fmtTimeAgo(unixSecs) {
    if (!unixSecs) return "";
    const ageSec = Math.max(0, Date.now() / 1000 - unixSecs);
    if (ageSec < 60) return `${Math.round(ageSec)}s ago`;
    if (ageSec < 3600) return `${Math.round(ageSec / 60)}m ago`;
    if (ageSec < 86400) return `${Math.round(ageSec / 3600)}h ago`;
    return `${Math.round(ageSec / 86400)}d ago`;
  }

  function updateWorkers(workers) {
    const names = Object.keys(workers).sort();
    workersEl.innerHTML = "";
    if (names.length === 0) {
      workersEl.innerHTML = '<div class="card state-unknown"><div class="card-header"><div class="name">No workers reporting yet</div></div></div>';
      return;
    }
    for (const name of names) {
      const w = workers[name];
      const state = w.state || (w.alive > 0 ? "alive" : "unknown");
      const card = document.createElement("div");
      card.className = `card state-${state}`;

      // For non-alive workers, fall back to the previously recorded alive count
      // so the user can see how many were running before things went bad.
      const aliveDisplay = w.alive > 0 ? w.alive : 0;
      const capacityDisplay = w.alive > 0 ? w.task_limit_total : (w.last_alive_count || 0);
      const backlog = w.backlog || 0;
      const utilization = w.utilization || 0;
      const utilWidth = Math.min(100, Math.round(utilization * 100));
      const utilClass = state === "crashed" ? "high" : utilizationClass(utilization);

      let metaText = "";
      if (state === "alive") {
        if (w.stale > 0) metaText = `${w.stale} stale heartbeat(s)`;
      } else if (state === "crashed") {
        metaText = `Crashed — last seen alive ${fmtTimeAgo(w.last_seen_alive)}`;
      } else if (state === "scaled_down") {
        metaText = `Scaled down — last seen alive ${fmtTimeAgo(w.last_seen_alive)}`;
      } else {
        metaText = "No heartbeat yet";
      }

      card.innerHTML = `
        <div class="card-header">
          <div class="name">${name}</div>
          <div class="state-pill ${state}">${state.replace("_", " ")}</div>
        </div>
        <div class="counts">
          <span><span class="label">Alive</span><span class="value alive">${aliveDisplay}</span></span>
          <span><span class="label">Backlog</span><span class="value">${fmt(backlog)}</span></span>
          <span><span class="label">Capacity</span><span class="value">${fmt(capacityDisplay)}</span></span>
        </div>
        <div class="util-bar"><div class="fill ${utilClass}" style="width: ${utilWidth}%"></div></div>
        <div class="meta">${metaText}</div>
      `;
      workersEl.appendChild(card);
    }
  }

  function ingestEvents(events, ts) {
    if (!events || events.length === 0) return;
    for (const ev of events) {
      eventBuffer.unshift({ ...ev, ts });
    }
    if (eventBuffer.length > EVENT_BUFFER_LIMIT) {
      eventBuffer.length = EVENT_BUFFER_LIMIT;
    }
    renderEvents();
  }

  function renderEvents() {
    if (eventBuffer.length === 0) {
      eventsEl.innerHTML = '<div class="empty">No scaling events yet.</div>';
      return;
    }
    eventsEl.innerHTML = eventBuffer
      .map(ev => {
        const when = new Date(ev.ts * 1000).toLocaleTimeString();
        let kindClass, kindLabel;
        if (ev.type === "scale_up") {
          kindClass = "scale_up";
          kindLabel = `+${ev.to - ev.from} → ${ev.to}`;
        } else if (ev.kind === "crashed") {
          kindClass = "scale_down_crashed";
          kindLabel = "crashed";
        } else if (ev.kind === "scaled_down") {
          kindClass = "scale_down_scaled_down";
          kindLabel = "scaled down";
        } else {
          kindClass = "scale_down_alive";
          kindLabel = `-${ev.from - ev.to} → ${ev.to}`;
        }
        return `<li>
          <span class="kind ${kindClass}">${kindLabel}</span>
          <span class="rule">${ev.worker}</span>
          <span class="when">${when}</span>
        </li>`;
      })
      .join("");
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
      hidden: xlenHiddenSeries.has(n),
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
      const res = await fetch("api/alerts?limit=50");
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
    ingestEvents(snap.events || [], snap.ts);
    if (snap.new_alerts && snap.new_alerts.length) {
      loadAlerts();
    }
  }

  // ---- transport ----------------------------------------------------------

  let ws;
  let pollTimer;

  function connect() {
    // Build the websocket URL from <base href>, so a path-prefixed deployment
    // (e.g. served at /monitor/) routes /monitor/ws correctly. Locally this
    // resolves to /ws since <base href="/">.
    const baseUrl = new URL("ws", document.baseURI);
    baseUrl.protocol = baseUrl.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(baseUrl.toString());
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
            const r = await fetch("api/snapshot");
            applySnapshot(await r.json());
          } catch (e) { /* swallow */ }
        }, 5000);
      }
      setTimeout(connect, 3000);
    };
    ws.onerror = () => { /* onclose will handle reconnect */ };
  }

  // Initial fetch so the UI is populated before the socket opens.
  fetch("api/snapshot")
    .then(r => r.json())
    .then(applySnapshot)
    .catch(() => {});
  loadAlerts();
  setInterval(loadAlerts, 30000);
  connect();
})();
