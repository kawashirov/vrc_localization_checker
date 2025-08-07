
DROP INDEX IF EXISTS latest_suggestions__unique;
DROP INDEX IF EXISTS latest_suggestions__source_id;
DROP INDEX IF EXISTS latest_suggestions__target_id;
DROP INDEX IF EXISTS latest_suggestions__target_id__model_id;
DROP MATERIALIZED VIEW latest_suggestions;

-- View: latest_suggestions
-- suggestions but only ones that can be found in latest_translations,
-- i.e. only newest versions, if string_body changes
-- and only with different bodies

CREATE MATERIALIZED VIEW latest_suggestions AS
SELECT s.*
FROM suggestions s
JOIN latest_translations lt_source ON lt_source.id = s.source_id
JOIN latest_translations lt_target ON lt_target.id = s.target_id
WHERE lt_source.string_body != lt_target.string_body;

ALTER TABLE latest_suggestions OWNER TO vrc_localization_checker;

CREATE UNIQUE INDEX IF NOT EXISTS  latest_suggestions__unique ON latest_suggestions (source_id, target_id, model_id, added_at);
CREATE INDEX IF NOT EXISTS latest_suggestions__source_id ON latest_suggestions (source_id);
CREATE INDEX IF NOT EXISTS latest_suggestions__target_id ON latest_suggestions (target_id);
CREATE INDEX IF NOT EXISTS latest_suggestions__target_id__model_id ON latest_suggestions (target_id, model_id);
