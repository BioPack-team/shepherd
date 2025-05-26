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
  callback_id varchar(255)
);
