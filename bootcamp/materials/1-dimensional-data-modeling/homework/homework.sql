-- 1. DDL for actors table
-- Create type films
DROP TYPE IF EXISTS films CASCADE;
CREATE TYPE films AS (
    film text,
    votes integer,
    rating real,
    filmid text
);

-- Create type quality_class
DROP TYPE IF EXISTS quality_class CASCADE;
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- Create table actors
DROP TABLE IF EXISTS actors;
CREATE TABLE actors (
    actor_id text NOT NULL,
    actor_name text NOT NULL,
    films films[] NOT NULL DEFAULT '{}',
    quality_class quality_class,
    is_active boolean NOT NULL DEFAULT false,
    current_year integer NOT NULL,
    PRIMARY KEY (actor_id, current_year)
);

-- 2. Cumulative table generation query
INSERT INTO actors (actor_id, actor_name, films, quality_class, is_active, current_year)
WITH last_year AS (
    SELECT *
    FROM actors
    WHERE current_year = 1969
),
this_year AS (
    SELECT
        actor_id,
        actor AS actor_name,
        AVG(rating) AS avg_rating,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films_this_year,
        year
    FROM actor_films
    WHERE year = 1970
    GROUP BY
        actor_id,
        actor
)
SELECT
    COALESCE(t.actor_id, l.actor_id) AS actor_id,
    COALESCE(t.actor_name, l.actor_name) AS actor_name,
    CASE
        WHEN l.actor_id IS NULL THEN t.films_this_year
        WHEN t.actor_id IS NOT NULL THEN l.films || t.films_this_year
        ELSE l.films
    END AS films,
    CASE
        WHEN t.actor_id IS NOT NULL THEN
            CASE
                WHEN t.avg_rating > 8 THEN 'star'
                WHEN t.avg_rating > 7 THEN 'good'
                WHEN t.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE l.quality_class
    END AS quality_class,
    (t.actor_id IS NOT NULL) AS is_active,
    COALESCE(t.year, l.current_year + 1) AS current_year
FROM
    this_year t
    FULL OUTER JOIN last_year l ON t.actor_id = l.actor_id;

-- 3. DDL for actors_history_scd table
DROP TABLE IF EXISTS actors_history_scd;
CREATE TABLE actors_history_scd (
    actor_id text NOT NULL,
    actor_name text NOT NULL,
    quality_class quality_class NOT NULL,
    start_date integer NOT NULL,
    end_date integer,
    is_active boolean NOT NULL,
    PRIMARY KEY (actor_id, start_date)
);

-- 4. Backfill query for actors_history_scd
INSERT INTO actors_history_scd (actor_id, actor_name, quality_class, start_date, end_date, is_active)
WITH with_previous AS (
    SELECT
        actor_id,
        actor_name,
        quality_class,
        is_active,
        current_year,
        LAG(quality_class) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
        LAG(is_active) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
    FROM
        actors
    WHERE
        current_year <= 2000
),
with_indicators AS (
    SELECT
        *,
        CASE
            WHEN previous_quality_class IS NULL AND previous_is_active IS NULL THEN 1
            WHEN quality_class IS DISTINCT FROM previous_quality_class THEN 1
            WHEN is_active IS DISTINCT FROM previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM
        with_previous
),
with_streaks AS (
    SELECT
        *,
        SUM(change_indicator) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
    FROM
        with_indicators
),
final_streaks AS (
    SELECT
        *,
        MAX(streak_identifier) OVER (PARTITION BY actor_id) AS max_streak_identifier
    FROM
        with_streaks
)
SELECT
    actor_id,
    actor_name,
    quality_class,
    MIN(current_year) AS start_date,
    CASE
        WHEN streak_identifier = max_streak_identifier THEN NULL
        ELSE MAX(current_year)
    END AS end_date,
    MIN(is_active) AS is_active
FROM
    final_streaks
GROUP BY
    actor_id,
    actor_name,
    quality_class,
    is_active,
    streak_identifier,
    max_streak_identifier
ORDER BY
    actor_id,
    start_date;

-- 5. Incremental query for actors_history_scd
WITH last_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE end_date IS NULL
),
historical_scd AS (
    SELECT
        actor_id,
        actor_name,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM
        actors_history_scd
    WHERE
        end_date IS NOT NULL
),
this_year_data AS (
    SELECT *
    FROM actors
    WHERE current_year = 2001
),
unchanged_records AS (
    SELECT
        ly.actor_id,
        ly.actor_name,
        ly.quality_class,
        ly.is_active,
        ly.start_date,
        NULL::integer AS end_date
    FROM
        this_year_data ty
        JOIN last_year_scd ly ON ty.actor_id = ly.actor_id
    WHERE
        ty.quality_class IS NOT DISTINCT FROM ly.quality_class
        AND ty.is_active IS NOT DISTINCT FROM ly.is_active
),
changed_records AS (
    SELECT
        ty.actor_id,
        ty.actor_name,
        r.quality_class,
        r.is_active,
        r.start_date,
        r.end_date
    FROM
        this_year_data ty
        JOIN last_year_scd ly USING (actor_id)
        CROSS JOIN LATERAL UNNEST(ARRAY[
            ROW(ly.quality_class, ly.is_active, ly.start_date, ty.current_year - 1),
            ROW(ty.quality_class, ty.is_active, ty.current_year, NULL::integer)
        ]) AS r (quality_class, is_active, start_date, end_date)
    WHERE
        ty.quality_class IS DISTINCT FROM ly.quality_class
        OR ty.is_active IS DISTINCT FROM ly.is_active
),
new_records AS (
    SELECT
        ty.actor_id,
        ty.actor_name,
        ty.quality_class,
        ty.is_active,
        ty.current_year AS start_date,
        NULL::integer AS end_date
    FROM
        this_year_data ty
        LEFT JOIN last_year_scd ly ON ty.actor_id = ly.actor_id
    WHERE
        ly.actor_id IS NULL
)
SELECT
    actor_id,
    actor_name,
    quality_class,
    is_active,
    start_date,
    end_date
FROM
    historical_scd
UNION ALL
SELECT
    actor_id,
    actor_name,
    quality_class,
    is_active,
    start_date,
    end_date
FROM
    unchanged_records
UNION ALL
SELECT
    actor_id,
    actor_name,
    quality_class,
    is_active,
    start_date,
    end_date
FROM
    changed_records
UNION ALL
SELECT
    actor_id,
    actor_name,
    quality_class,
    is_active,
    start_date,
    end_date
FROM
    new_records;