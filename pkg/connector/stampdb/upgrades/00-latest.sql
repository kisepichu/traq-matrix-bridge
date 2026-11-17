-- v0 -> v1: Latest revision

CREATE TABLE traq_stamp (
	id           TEXT    PRIMARY KEY,
	name         TEXT    NOT NULL,
	is_unicode   BOOLEAN NOT NULL,
	emoji        TEXT    NOT NULL DEFAULT '',
	-- A Unicode stamp whose emoji cannot be derived from its file; it is not resolved again until it changes.
	unresolvable BOOLEAN NOT NULL DEFAULT false,
	file_id      TEXT    NOT NULL
);
