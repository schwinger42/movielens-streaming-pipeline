import pandas as pd
import os
import logging
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths
GOLD_PATH = "output/medallion/gold"
DWH_PATH = "output/dwh"

def setup_directories():
    """Create output directories"""
    os.makedirs(DWH_PATH, exist_ok=True)
    logger.info(f"Created directory: {DWH_PATH}")

def load_movie_analytics():
    """Load movie analytics data from Gold layer"""
    try:
        movie_analytics_path = f"{GOLD_PATH}/movie_analytics.parquet"
        logger.info(f"Loading movie analytics from: {movie_analytics_path}")
        
        df = pd.read_parquet(movie_analytics_path)
        logger.info(f"Loaded {len(df)} movie analytics records")
        return df
    except Exception as e:
        logger.error(f"Error loading movie analytics: {e}")
        return None

def create_top_movies_by_rating(df, min_ratings=50):
    """Create a dataset of top movies by rating with a minimum number of ratings"""
    try:
        # Filter out movies with too few ratings
        filtered_df = df[df['num_ratings'] >= min_ratings].copy()
        
        # Sort by average rating
        top_movies = filtered_df.sort_values('avg_rating', ascending=False).head(100)
        
        # Save to CSV and JSON
        top_movies.to_csv(f"{DWH_PATH}/top_movies_by_rating.csv", index=False)
        
        # Convert to JSON format suitable for visualization
        result = []
        for _, row in top_movies.head(20).iterrows():
            result.append({
                'title': row['title'],
                'avg_rating': round(row['avg_rating'], 2),
                'num_ratings': int(row['num_ratings'])
            })
        
        with open(f"{DWH_PATH}/top_movies_by_rating.json", 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"Created top movies dataset with {len(top_movies)} records")
        return top_movies
    except Exception as e:
        logger.error(f"Error creating top movies dataset: {e}")
        return None

def create_rating_distribution(df):
    """Create a dataset of rating distribution"""
    try:
        # Create rating distribution
        rating_dist = df['rating_tier'].value_counts().reset_index()
        rating_dist.columns = ['rating_tier', 'count']
        
        # Save to CSV and JSON
        rating_dist.to_csv(f"{DWH_PATH}/rating_distribution.csv", index=False)
        
        # Convert to JSON format suitable for visualization
        result = []
        for _, row in rating_dist.iterrows():
            if pd.notna(row['rating_tier']):
                result.append({
                    'tier': row['rating_tier'],
                    'count': int(row['count'])
                })
        
        with open(f"{DWH_PATH}/rating_distribution.json", 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"Created rating distribution dataset")
        return rating_dist
    except Exception as e:
        logger.error(f"Error creating rating distribution dataset: {e}")
        return None

def create_year_analysis(df):
    """Create a dataset of movies by year"""
    try:
        # Convert year to numeric, handling errors
        df['year_num'] = pd.to_numeric(df['year'], errors='coerce')
        
        # Group by year
        year_analysis = df.groupby('year_num').agg(
            movie_count=('movieId', 'count'),
            avg_rating=('avg_rating', 'mean'),
            total_ratings=('num_ratings', 'sum')
        ).reset_index()
        
        # Filter valid years (from 1900 to current year)
        current_year = datetime.now().year
        year_analysis = year_analysis[(year_analysis['year_num'] >= 1900) & 
                                     (year_analysis['year_num'] <= current_year)]
        
        # Sort by year
        year_analysis = year_analysis.sort_values('year_num')
        
        # Save to CSV and JSON
        year_analysis.to_csv(f"{DWH_PATH}/movies_by_year.csv", index=False)
        
        # Convert to JSON format suitable for visualization
        result = []
        for _, row in year_analysis.iterrows():
            if pd.notna(row['year_num']):
                result.append({
                    'year': int(row['year_num']),
                    'movie_count': int(row['movie_count']),
                    'avg_rating': round(row['avg_rating'], 2),
                    'total_ratings': int(row['total_ratings'])
                })
        
        with open(f"{DWH_PATH}/movies_by_year.json", 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"Created year analysis dataset with {len(year_analysis)} records")
        return year_analysis
    except Exception as e:
        logger.error(f"Error creating year analysis dataset: {e}")
        return None

def create_metabase_queries():
    """Create SQL queries for Metabase"""
    try:
        queries = {
            "top_movies": """
SELECT 
    title, 
    avg_rating, 
    num_ratings
FROM 
    gold_movie_analytics
WHERE 
    num_ratings >= 50
ORDER BY 
    avg_rating DESC
LIMIT 20;
""",
            "rating_distribution": """
SELECT 
    rating_tier, 
    COUNT(*) as count
FROM 
    gold_movie_analytics
GROUP BY 
    rating_tier
ORDER BY 
    rating_tier;
""",
            "movies_by_year": """
SELECT 
    year, 
    COUNT(*) as movie_count,
    AVG(avg_rating) as avg_rating,
    SUM(num_ratings) as total_ratings
FROM 
    gold_movie_analytics
WHERE 
    year IS NOT NULL
    AND year BETWEEN '1900' AND '2025'
GROUP BY 
    year
ORDER BY 
    year;
"""
        }
        
        # Save queries to files
        os.makedirs(f"{DWH_PATH}/metabase_queries", exist_ok=True)
        for name, query in queries.items():
            with open(f"{DWH_PATH}/metabase_queries/{name}.sql", 'w') as f:
                f.write(query)
        
        logger.info(f"Created Metabase queries")
        return True
    except Exception as e:
        logger.error(f"Error creating Metabase queries: {e}")
        return False

def main():
    """Main function to query and prepare data for the data warehouse"""
    try:
        logger.info("Starting data warehouse preparation")
        
        # Setup directories
        setup_directories()
        
        # Load movie analytics
        df = load_movie_analytics()
        if df is None:
            logger.error("Failed to load movie analytics data")
            return
        
        # Create datasets
        create_top_movies_by_rating(df)
        create_rating_distribution(df)
        create_year_analysis(df)
        
        # Create Metabase queries
        create_metabase_queries()
        
        logger.info("Data warehouse preparation completed")
    
    except Exception as e:
        logger.error(f"Error preparing data warehouse: {e}")

if __name__ == "__main__":
    main()
