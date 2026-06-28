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

-- ARS (Autonomous Relay System) tables --
-- NOTE: base_routes.py reads shepherd_brain by positional index ([7]/[8]/[9]),
-- so the column appended below MUST stay at the end of the table.
ALTER TABLE shepherd_brain ADD COLUMN IF NOT EXISTS is_ars_parent BOOLEAN DEFAULT FALSE;
-- Single-winner latch: the first ars_accumulate task that observes all ARAs
-- done flips this TRUE and launches the post-merge tail workflow. Atomic
-- ``UPDATE ... WHERE ars_tail_launched = FALSE RETURNING`` guarantees exactly
-- one launch even when child callbacks land concurrently.
ALTER TABLE shepherd_brain ADD COLUMN IF NOT EXISTS ars_tail_launched BOOLEAN DEFAULT FALSE;
-- Retain latch (ARS parity): when TRUE the query (and its ARS children) are
-- excluded from the purge janitor so a user can preserve a result indefinitely.
ALTER TABLE shepherd_brain ADD COLUMN IF NOT EXISTS retain BOOLEAN DEFAULT FALSE;

-- One row per (parent ARS query x ARA). This is the cross-ARA completion
-- counter: a parent query is "done fanning out" when no child row is left in a
-- non-terminal state. Modeled on the callbacks/get_running_callbacks primitive.
CREATE TABLE IF NOT EXISTS ars_children (
  parent_qid        varchar(255) REFERENCES shepherd_brain(qid) ON DELETE CASCADE,
  ara               TEXT NOT NULL,            -- aragorn | arax | bte | sipr
  child_qid         varchar(255),            -- the child query_id we dispatched
  child_response_id TEXT,                    -- where that ARA's merged response lives
  ars_callback_id   TEXT,                    -- callback id the child posts back to
  otel_trace        TEXT,                    -- otel carrier to continue the trace
  status            TEXT NOT NULL,           -- QUEUED | RUNNING | DONE | ERROR
  code              INT,                     -- HTTP-ish status code (ARS parity)
  result_count      INT,
  merged_version    INT DEFAULT 0,
  start_time        TIMESTAMP DEFAULT NOW(),
  stop_time         TIMESTAMP,
  PRIMARY KEY (parent_qid, ara)
);
CREATE INDEX IF NOT EXISTS idx_ars_children_parent ON ars_children (parent_qid);
CREATE INDEX IF NOT EXISTS idx_ars_children_callback ON ars_children (ars_callback_id);

-- Actor/agent/channel registry for auto-discovery (SmartAPI parity).
CREATE TABLE IF NOT EXISTS ars_actors (
  id         SERIAL PRIMARY KEY,
  infores    TEXT UNIQUE,
  url        TEXT,
  channel    TEXT,
  agent_name TEXT,
  maturity   TEXT,
  active     BOOLEAN DEFAULT TRUE,
  inforev    JSONB
);

-- Subscribers that receive websocket/status push for a parent query.
CREATE TABLE IF NOT EXISTS ars_subscribers (
  id           SERIAL PRIMARY KEY,
  parent_qid   varchar(255),
  callback_url TEXT,
  created      TIMESTAMP DEFAULT NOW()
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

