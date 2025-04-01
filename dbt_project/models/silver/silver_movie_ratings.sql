WITH movie_ratings AS (
    SELECT
        m.movieId,
        m.title,
        m.genres,
        AVG(r.rating) AS avg_rating,
        COUNT(r.rating) AS rating_count
    FROM {{ source('silver', 'movies') }} m
    LEFT JOIN {{ source('silver', 'ratings') }} r
        ON m.movieId = r.movieId
    GROUP BY 1, 2, 3
)

SELECT * FROM movie_ratings
