-- Translator ARS tables (mirror of the upstream Django models).
-- This file is the same DDL that shepherd_db/init_db.sql carries; it is
-- bundled with shepherd_utils so apply_schema_upgrades can bring a
-- pre-existing Postgres volume (whose init_db.sql ran before the ARS port
-- landed) up to date at startup. Everything here is idempotent.

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
