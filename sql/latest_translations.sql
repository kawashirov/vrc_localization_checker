
DROP MATERIALIZED VIEW latest_translations;

-- View: latest_translations
-- translations but only with greatest added_at per each (string_file, string_key, lang_code),
-- i.e. only newest versions, if string_body changes

CREATE MATERIALIZED VIEW latest_translations AS
SELECT *
FROM (
	SELECT
		*,
		row_number() OVER (
			PARTITION BY string_file, string_key, lang_code
			ORDER BY translations.added_at DESC
		) AS version_rank
	FROM translations
) t
WHERE version_rank = 1;

ALTER TABLE latest_translations OWNER TO vrc_localization_checker;

CREATE UNIQUE INDEX IF NOT EXISTS latest_translations__unique ON latest_translations (string_file, string_key, lang_code);
CREATE INDEX IF NOT EXISTS latest_translations__lang_code__added_at ON latest_translations (lang_code, added_at);
