WITH
pairs AS (
	SELECT
		lt_target.string_file,
		lt_target.string_key,
		lt_source.id AS source_id,
		lt_target.id AS target_id,
		lt_source.string_body AS source_string_body,
		lt_target.string_body AS target_string_body,
		lt_target.added_at AS source_added_at,
		lt_source.added_at AS target_added_at
	FROM latest_translations lt_target
	JOIN latest_translations lt_source
	ON lt_target.string_file = lt_source.string_file AND lt_target.string_key = lt_source.string_key
	WHERE lt_source.lang_code = :source_lang AND lt_target.lang_code = :target_lang AND lt_source.string_body != lt_target.string_body
),
sugg_counts AS (
	SELECT
		source_id,
		target_id,
		COUNT(added_at) AS cnt
	FROM latest_suggestions
	WHERE model_id = :model_id
	GROUP BY source_id, target_id
),
joined AS (
	SELECT
		p.string_file,
		p.string_key,
		p.source_id,
		p.target_id,
		p.source_string_body,
		p.target_string_body,
		p.source_added_at,
		p.target_added_at,
		COALESCE(sc.cnt, 0) AS suggestions_count
	FROM pairs p
	LEFT JOIN sugg_counts sc
	ON p.source_id = sc.source_id AND p.target_id = sc.target_id
)
SELECT *
FROM joined
WHERE suggestions_count < :M
ORDER BY suggestions_count ASC, target_added_at ASC, source_added_at ASC
LIMIT :N;
