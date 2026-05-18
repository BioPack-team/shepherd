-- Initialize the Shepherd POSTGRES tables --

CREATE TABLE IF NOT EXISTS shepherd_brain (
  qid varchar(255) PRIMARY KEY,
  start_time TIMESTAMP,
  stop_time TIMESTAMP,
  submitter TEXT,
  remote_ip TEXT,
  domain TEXT,
  hostname TEXT,
  response_id TEXT,
  callback_url TEXT,
  state TEXT,
  status TEXT,
  description TEXT
);

CREATE TABLE IF NOT EXISTS callbacks (
  query_id varchar(255) REFERENCES shepherd_brain(qid),
  callback_id varchar(255),
  otel_trace varchar(255)
);

-- Historical metrics archive, written by the monitor every 30s. Used by the
-- History tab to show trends over days/weeks. Live dashboard reads from Redis
-- (recent, fast); this is the durable 30-day record.
CREATE TABLE IF NOT EXISTS monitor_metrics (
  ts TIMESTAMPTZ NOT NULL,
  metric TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (metric, ts)
);
CREATE INDEX IF NOT EXISTS idx_monitor_metrics_ts ON monitor_metrics (ts);

-- Discrete events (scale_up, scale_down, crash, alert) keyed by autoincrement
-- so simultaneous events at the same instant don't collide.
CREATE TABLE IF NOT EXISTS monitor_events (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  type TEXT NOT NULL,
  worker TEXT,
  severity TEXT,
  detail TEXT,
  payload JSONB
);
CREATE INDEX IF NOT EXISTS idx_monitor_events_ts ON monitor_events (ts);
CREATE INDEX IF NOT EXISTS idx_monitor_events_type_ts ON monitor_events (type, ts);

-- Per-stream task latency, aggregated into 30s buckets by the monitor.
CREATE TABLE IF NOT EXISTS monitor_task_latency (
  ts TIMESTAMPTZ NOT NULL,
  stream TEXT NOT NULL,
  count INT NOT NULL,
  mean_ms DOUBLE PRECISION,
  p50_ms DOUBLE PRECISION,
  p90_ms DOUBLE PRECISION,
  p95_ms DOUBLE PRECISION,
  p99_ms DOUBLE PRECISION,
  min_ms DOUBLE PRECISION,
  max_ms DOUBLE PRECISION,
  PRIMARY KEY (stream, ts)
);
CREATE INDEX IF NOT EXISTS idx_monitor_latency_ts ON monitor_task_latency (ts);

