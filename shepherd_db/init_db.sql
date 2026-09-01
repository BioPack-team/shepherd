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

-- Every /callback request looks a row up by callback_id (and deletes it once
-- processed), and the lookup workers poll by query_id to decide when a query's
-- fan-out is done. Without these indexes both are sequential scans whose cost
-- grows with concurrent load, so each one holds a pool connection longer and
-- starves the small per-process connection pools. These are also applied
-- idempotently at startup (shepherd_utils.db.initialize_db) for deployments
-- whose data volume predates this file change.
CREATE INDEX IF NOT EXISTS idx_callbacks_callback_id ON callbacks (callback_id);
CREATE INDEX IF NOT EXISTS idx_callbacks_query_id ON callbacks (query_id);

-- ---------------------------------------------------------------------------
-- Translator ARS tables. These mirror the Django models of the upstream ARS
-- (NCATSTranslator/Relay tr_ars app) so the /ars API surface and message
-- lifecycle can be served from Shepherd with identical observable behavior.
-- ---------------------------------------------------------------------------

-- Integer pks mirror Django's implicit AutoField so serialized envelopes
-- ({"model": ..., "pk": <int>}) match the upstream wire shape.
CREATE TABLE IF NOT EXISTS ars_agent (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  uri TEXT NOT NULL DEFAULT '',
  contact TEXT,
  registered TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ars_channel (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  description TEXT
);

-- Actor.channel stores the Django-serialized channel list
-- ([{"model":"tr_ars.channel","pk":1,"fields":{...}}, ...]) exactly as the
-- upstream get_or_create_actor persists it -- trace/actors rendering reads
-- ch['fields']['name'] from it.
CREATE TABLE IF NOT EXISTS ars_actor (
  id SERIAL PRIMARY KEY,
  agent INT NOT NULL REFERENCES ars_agent(id) ON DELETE CASCADE,
  channel JSONB NOT NULL DEFAULT '[]',
  path TEXT NOT NULL DEFAULT '',
  inforesid TEXT NOT NULL DEFAULT '',
  active BOOLEAN NOT NULL DEFAULT TRUE,
  UNIQUE (agent, path)
);

-- The ARS message tree: one parent row per submitted query, one child row per
-- actor fan-out, plus merge-child rows (agent ars-ars-agent). Payload blobs
-- live in Redis (hot path) with a durable zstd copy written into ``data`` when
-- a message reaches a terminal status.
CREATE TABLE IF NOT EXISTS ars_message (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  code SMALLINT NOT NULL DEFAULT 200,
  status CHAR(1) NOT NULL DEFAULT 'U',
  actor INT NOT NULL REFERENCES ars_actor(id),
  ref UUID REFERENCES ars_message(id),
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  url TEXT,
  result_count INT,
  result_stat JSONB,
  retain BOOLEAN NOT NULL DEFAULT FALSE,
  merge_semaphore BOOLEAN NOT NULL DEFAULT FALSE,
  merged_version UUID REFERENCES ars_message(id),
  merged_versions_list JSONB,
  params JSONB,
  data BYTEA
);
CREATE INDEX IF NOT EXISTS idx_ars_message_ref ON ars_message (ref);
CREATE INDEX IF NOT EXISTS idx_ars_message_status_updated ON ars_message (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_ars_message_ts ON ars_message (ts);

CREATE TABLE IF NOT EXISTS ars_client (
  id SERIAL PRIMARY KEY,
  client_id TEXT NOT NULL UNIQUE,
  client_secret TEXT NOT NULL,
  callback_url TEXT NOT NULL,
  date_created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  date_secret_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  active BOOLEAN NOT NULL DEFAULT FALSE,
  -- Upstream keeps a JSON list of subscribed message pks on the client in
  -- addition to the M2M join table; both are maintained together.
  subscriptions JSONB
);

CREATE TABLE IF NOT EXISTS ars_subscription (
  client_id INT REFERENCES ars_client(id) ON DELETE CASCADE,
  message_id UUID REFERENCES ars_message(id) ON DELETE CASCADE,
  PRIMARY KEY (client_id, message_id)
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

