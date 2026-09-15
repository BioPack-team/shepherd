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

-- The ARS message tree: one parent row per submitted query, one child row per
-- ARA fan-out, plus merge-child rows. ``agent`` names who the row belongs to
-- (ars-default-agent for a parent, ara-shepherd-<ara> for an ARA's child,
-- ars-ars-agent for a merged message); the ARAs themselves are a static
-- roster in shepherd_utils/ars/aras.py, not a table -- the ARS only talks to
-- the ARAs this Shepherd deployment hosts. Payload blobs live in Redis (hot
-- path) with a durable zstd copy written into ``data`` when a message reaches
-- a terminal status.
CREATE TABLE IF NOT EXISTS ars_message (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  code SMALLINT NOT NULL DEFAULT 200,
  status CHAR(1) NOT NULL DEFAULT 'U',
  agent TEXT NOT NULL DEFAULT '',
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
-- idx_ars_message_agent doubles as the schema-upgrade marker for the
-- registry retirement (shepherd_utils.db.apply_schema_upgrades migrates a
-- volume still carrying ars_message.actor + the registry tables first).
CREATE INDEX IF NOT EXISTS idx_ars_message_agent ON ars_message (agent, ts);

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

-- ---------------------------------------------------------------------------
-- ARS response cache (Shepherd-native; upstream has none). The cache stores
-- no payloads of its own: it indexes a canonical query-graph hash to the
-- parent pk of the one completed message tree that answers it (the "source
-- tree"), whose blobs already live in ars_message.data. A hit returns that
-- pk. See docs/ARS_RESPONSE_CACHE_PLAN.md.
-- ---------------------------------------------------------------------------

-- Singleton generation counter. Bumping it invalidates every entry at once
-- (lookups filter on the current generation); superseded rows are purged
-- lazily by the watchdog and their source trees then fall under the normal
-- payload retention window.
CREATE TABLE IF NOT EXISTS ars_cache_meta (
  id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  generation INT NOT NULL DEFAULT 1,
  bumped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  bumped_reason TEXT
);
INSERT INTO ars_cache_meta (id) VALUES (TRUE) ON CONFLICT DO NOTHING;

-- 'pending' while the leader query runs (identical submits are handed the
-- leader's pk), 'ready' once its tree is complete. label_map records
-- the source query graph's node/edge/path ids -> canonical ids so a hit can
-- rewrite bindings to the caller's own ids.
CREATE TABLE IF NOT EXISTS ars_response_cache (
  generation INT NOT NULL,
  cache_key TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'ready')),
  source_pk UUID NOT NULL REFERENCES ars_message(id),
  label_map JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ready_at TIMESTAMPTZ,
  hit_count INT NOT NULL DEFAULT 0,
  last_hit_at TIMESTAMPTZ,
  PRIMARY KEY (generation, cache_key)
);
-- idx_ars_response_cache_source doubles as the schema-upgrade marker for
-- this block (shepherd_utils.db.apply_schema_upgrades).
CREATE INDEX IF NOT EXISTS idx_ars_response_cache_source ON ars_response_cache (source_pk);
CREATE INDEX IF NOT EXISTS idx_ars_response_cache_state_created ON ars_response_cache (state, created_at);


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

