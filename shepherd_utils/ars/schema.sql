-- Translator ARS tables (Shepherd's de-federated port of the upstream
-- Django models).
-- This file is the same DDL that shepherd_db/init_db.sql carries; it is
-- bundled with shepherd_utils so apply_schema_upgrades can bring a
-- pre-existing Postgres volume (whose init_db.sql ran before the ARS port
-- landed, or before the registry tables were retired) up to date at
-- startup. Everything here is idempotent.

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

