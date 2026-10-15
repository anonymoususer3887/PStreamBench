-- Trending Topics (Q1): extract @mentions/#hashtags from tweet text, count
-- occurrences over a sliding window, keep only topics above a popularity
-- threshold.
--
-- Mirrors com.kom.dsp.TrendingTopics.TrendingTopics (Flink) query 1 (see
-- TwitterParser.java / TopicExtractor.java): parses a comma-separated tweet
-- record (id=field 2, timestamp=field 3, text=field 6 -- 1-indexed;
-- TwitterParser.java naively splits the WHOLE line on comma, which would
-- misalign fields if the tweet text itself contains a comma -- replicated
-- here as-is for comparability, not "fixed", same convention as this
-- project's other faithfully-preserved parser quirks), tokenizes the text
-- on whitespace, keeps tokens starting with "@" or "#" (stripping that
-- leading character), counts occurrences per topic over a sliding
-- processing-time window, and keeps only topics whose count exceeds
-- job_threshold (genuinely wired through in the reference implementation,
-- like Bargain Index -- not a hardcoded-and-ignored quirk like Machine
-- Outlier's).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every Phase 1 workload. Specific open
-- questions for this file:
--   - unnest(regexp_split_to_array(text, '\s+')) as the tokenizer --
--     regexp_split_to_array itself is confirmed working live (used during
--     Word Count's original -- since superseded -- draft), but not
--     specifically combined with unnest() + a LIKE filter in a streaming
--     context.
--   - HAVING on a streaming aggregate's result (filtering by
--     job_threshold) -- not independently confirmed; every other workload
--     here uses an outer WHERE over a CTE instead, which is a safer
--     fallback if HAVING doesn't work as expected in a materialized view.
--   - HOP argument order (as in every other windowed workload here).

CREATE SOURCE IF NOT EXISTS trending_topics_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'TTIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW trending_topics_mv AS
WITH parsed AS (
    SELECT
        pt,
        split_part(convert_from(line, 'UTF8'), ',', 6) AS text
    FROM trending_topics_source
),
topics AS (
    SELECT
        pt,
        substring(word, 2) AS topic
    FROM parsed, unnest(regexp_split_to_array(text, '\s+')) AS word
    WHERE word LIKE '@%' OR word LIKE '#%'
)
SELECT
    topic,
    COUNT(*) AS topic_count
FROM HOP(topics, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY topic, window_start, window_end
HAVING COUNT(*) > 5;

CREATE SINK IF NOT EXISTS trending_topics_sink
FROM trending_topics_mv
WITH (
    connector = 'kafka',
    topic = 'TTOut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
