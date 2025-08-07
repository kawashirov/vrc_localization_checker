
DROP INDEX IF EXISTS suggestions__source_id;
DROP INDEX IF EXISTS suggestions__target_id;
DROP TABLE IF EXISTS suggestions;

-- Table: suggestions
-- stores suggestions made by AIs, both current and previous, if their bodies changes

CREATE TABLE IF NOT EXISTS suggestions (
	source_id bigint NOT NULL REFERENCES translations(id),
	target_id bigint NOT NULL REFERENCES translations(id),
	model_id text NOT NULL,
	added_at timestamp with time zone NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'), -- basically the ID of suggestion, might be multiple per body pairs
	-- no info on used extra languages yet
	suggestion_string_body text, -- might be null if nothing suggested ('has_suggestion': False)
	suggestion_comment text, -- might be null if nothing suggested ('has_suggestion': False)
	"interval" interval NOT NULL,
	-- OpenAI metadata
	completion_tokens int, -- might be null if OpenAI API isn't fully compatible
	prompt_tokens int, -- might be null if OpenAI API isn't fully compatible
	system_fingerprint text, -- might be null if OpenAI API isn't fully compatible
	--
	CONSTRAINT suggestions_pkey PRIMARY KEY (source_id, target_id, model_id, added_at)
);

ALTER TABLE IF EXISTS suggestions OWNER to vrc_localization_checker;

CREATE INDEX IF NOT EXISTS suggestions__source_id ON suggestions (source_id);
CREATE INDEX IF NOT EXISTS suggestions__target_id ON suggestions (target_id);
