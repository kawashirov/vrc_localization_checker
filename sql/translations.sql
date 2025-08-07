
DROP INDEX IF EXISTS translations_pk_btree;
DROP TABLE IF EXISTS translations;

-- Table: translations
-- stores translations, both current and previous, if their bodies changes

CREATE TABLE IF NOT EXISTS translations (
	id bigserial PRIMARY KEY,
	string_file text NOT NULL,
	string_key text NOT NULL,
	lang_code text NOT NULL,
	string_body text NOT NULL,
	added_at timestamp with time zone NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
	 -- string bodies might change, we should track versions
	CONSTRAINT translations_unique UNIQUE (string_file, string_key, lang_code, string_body)
);

ALTER TABLE IF EXISTS translations OWNER to vrc_localization_checker;

CREATE INDEX IF NOT EXISTS translations__unique_btree ON translations (string_file, string_key, lang_code, string_body);
