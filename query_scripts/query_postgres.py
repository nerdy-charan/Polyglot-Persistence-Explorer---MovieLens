import pandas as pd
import subprocess

print("=" * 80)
print("POSTGRESQL QUERIES - Analytical Strengths")
print("=" * 80)

# Query 1: Top 10 Highest Rated Movies
print("\nQuery 1: Top 10 Highest Rated Movies (min 100 ratings)")
print("-" * 80)

query1 = """
SELECT 
    title,
    num_ratings,
    ROUND(avg_rating::numeric, 2) as avg_rating,
    genres
FROM movie_stats
WHERE num_ratings >= 100
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 10;
"""

result = subprocess.run([
    'docker', 'exec', 'movielens_postgres',
    'psql', '-U', 'admin', '-d', 'movielens', '-c', query1
], capture_output=True, text=True)

print(result.stdout)

# Query 2: Rating Trends Over Time
print("\nQuery 2: Average Ratings by Year")
print("-" * 80)

query2 = """
SELECT 
    year,
    SUM(num_ratings) as total_ratings,
    ROUND(AVG(avg_rating)::numeric, 2) as avg_rating
FROM temporal_analysis
GROUP BY year
ORDER BY year DESC
LIMIT 10;
"""

result = subprocess.run([
    'docker', 'exec', 'movielens_postgres',
    'psql', '-U', 'admin', '-d', 'movielens', '-c', query2
], capture_output=True, text=True)

print(result.stdout)

# Query 3: Genre Statistics
print("\nQuery 3: Most Rated Genres")
print("-" * 80)

query3 = """
SELECT 
    UNNEST(string_to_array(genres, '|')) as genre,
    COUNT(*) as movie_count,
    ROUND(AVG(avg_rating)::numeric, 2) as avg_rating,
    SUM(num_ratings) as total_ratings
FROM movie_stats
WHERE genres IS NOT NULL
GROUP BY genre
ORDER BY total_ratings DESC
LIMIT 10;
"""

result = subprocess.run([
    'docker', 'exec', 'movielens_postgres',
    'psql', '-U', 'admin', '-d', 'movielens', '-c', query3
], capture_output=True, text=True)

print(result.stdout)

# Query 4: Rating Distribution Analysis
print("\nQuery 4: Overall Rating Distribution")
print("-" * 80)

query4 = """
SELECT 
    rating,
    SUM(count) as total_count,
    ROUND((SUM(count)::numeric / (SELECT SUM(count) FROM rating_distribution) * 100), 2) as percentage
FROM rating_distribution
GROUP BY rating
ORDER BY rating DESC;
"""

result = subprocess.run([
    'docker', 'exec', 'movielens_postgres',
    'psql', '-U', 'admin', '-d', 'movielens', '-c', query4
], capture_output=True, text=True)

print(result.stdout)

# Export results to CSV
print("\nExporting query results to CSV...")

# Export top movies
export_query = """
COPY (
    SELECT title, num_ratings, ROUND(avg_rating::numeric, 2) as avg_rating, genres
    FROM movie_stats
    WHERE num_ratings >= 100
    ORDER BY avg_rating DESC
    LIMIT 50
) TO STDOUT CSV HEADER;
"""

result = subprocess.run([
    'docker', 'exec', 'movielens_postgres',
    'psql', '-U', 'admin', '-d', 'movielens', '-c', export_query
], capture_output=True, text=True)

with open('postgres_top_movies.csv', 'w') as f:
    f.write(result.stdout)

print("✓ Saved: postgres_top_movies.csv")

print("\n" + "=" * 80)
print("PostgreSQL demonstrates:")
print("  ✓ Fast aggregations (AVG, SUM, COUNT)")
print("  ✓ Complex JOINs and GROUP BY")
print("  ✓ Time-series analysis")
print("  ✓ Statistical calculations")
print("=" * 80)
