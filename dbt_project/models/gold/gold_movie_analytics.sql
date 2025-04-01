WITH movie_analytics AS (
    SELECT
        movieId,
        title,
        genres,
        avg_rating,
        rating_count,
        CASE
            WHEN avg_rating >= 4.0 THEN 'High'
            WHEN avg_rating >= 3.0 THEN 'Medium'
            ELSE 'Low'
        END AS rating_category,
        SPLIT(genres, '|') AS genre_array
    FROM {{ ref('silver_movie_ratings') }}
)

SELECT
    movieId,
    title,
    genres,
    avg_rating,
    rating_count,
    rating_category,
    g AS genre
FROM movie_analytics
CROSS JOIN UNNEST(genre_array) AS t(g)
WHERE rating_count >= 100  -- Only include movies with significant ratings
