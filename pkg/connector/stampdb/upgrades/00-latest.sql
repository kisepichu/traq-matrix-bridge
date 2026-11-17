-- v0 -> v1: Latest revision

CREATE TABLE traq_stamp (
	id         TEXT    PRIMARY KEY,
	name       TEXT    NOT NULL,
	is_unicode BOOLEAN NOT NULL,
	emoji      TEXT    NOT NULL DEFAULT '',
	file_id    TEXT    NOT NULL
);
