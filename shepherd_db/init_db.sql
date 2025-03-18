-- Initialize the Shepherd Brain table --

CREATE TABLE IF NOT EXISTS shepherd_brain (
  pk varchar(255) PRIMARY KEY,
  initial_message bytea,
  final_message bytea,
  ara_data TEXT,
  pipeline TEXT
);
