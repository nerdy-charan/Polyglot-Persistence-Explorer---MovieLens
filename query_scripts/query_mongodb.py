from pymongo import MongoClient
import pandas as pd
import json

print("=" * 80)
print("MONGODB QUERIES - Document Flexibility")
print("=" * 80)

client = MongoClient('mongodb://admin:password@localhost:27017/', authSource='admin')
db = client['movielens']
collection = db['movies']

# Query 1: Find Action Movies with High Ratings
print("\nQuery 1: Action Movies with Rating > 4.0")
print("-" * 80)

results = collection.find(
    {
        'genres': {'$in': ['Action']},
        'ratings.average': {'$gte': 4.0},
        'ratings.count': {'$gte': 50}
    },
    {
        'title': 1,
        'genres': 1,
        'ratings.average': 1,
        'ratings.count': 1,
        '_id': 0
    }
).sort('ratings.average', -1).limit(10)

for movie in results:
    print(f"{movie['title']}: {movie['ratings']['average']:.2f} ({movie['ratings']['count']} ratings)")

# Query 2: Movies with Most Tags
print("\nQuery 2: Movies with Most User Tags")
print("-" * 80)

pipeline = [
    {'$project': {
        'title': 1,
        'num_tags': {'$size': '$tags'},
        'genres': 1
    }},
    {'$match': {'num_tags': {'$gt': 0}}},
    {'$sort': {'num_tags': -1}},
    {'$limit': 10}
]

results = collection.aggregate(pipeline)

for movie in results:
    print(f"{movie['title']}: {movie['num_tags']} tags ({movie.get('genres', [])})")

# Query 3: Genre Distribution using Aggregation
print("\nQuery 3: Movies Count by Genre")
print("-" * 80)

pipeline = [
    {'$unwind': '$genres'},
    {'$group': {
        '_id': '$genres',
        'count': {'$sum': 1},
        'avg_rating': {'$avg': '$ratings.average'}
    }},
    {'$sort': {'count': -1}},
    {'$limit': 10}
]

results = collection.aggregate(pipeline)

for genre in results:
    avg = genre['avg_rating'] if genre['avg_rating'] else 0
    print(f"{genre['_id']}: {genre['count']} movies (avg: {avg:.2f})")

# Query 4: Complex Nested Query - Highly Rated Drama with Specific Rating Distribution
print("\nQuery 4: Drama Movies with Diverse Rating Distribution")
print("-" * 80)

results = collection.find(
    {
        'genres': 'Drama',
        'ratings.average': {'$gte': 3.5},
        'ratings.distribution.5.0': {'$exists': True},
        'ratings.distribution.1.0': {'$exists': True}
    },
    {
        'title': 1,
        'ratings.average': 1,
        'ratings.count': 1,
        '_id': 0
    }
).sort('ratings.count', -1).limit(10)

for movie in results:
    print(f"{movie['title']}: {movie['ratings']['average']:.2f} ({movie['ratings']['count']} ratings)")

# Export to JSON
print("\nExporting query results...")

export_results = list(collection.find(
    {'ratings.count': {'$gte': 50}},
    {'_id': 0, 'title': 1, 'genres': 1, 'ratings': 1, 'tags': 1}
).sort('ratings.average', -1).limit(100))

with open('mongodb_top_movies.json', 'w') as f:
    json.dump(export_results, f, indent=2)

print("✓ Saved: mongodb_top_movies.json")

# Also save as CSV for Tableau
df = pd.DataFrame(export_results)
df['avg_rating'] = df['ratings'].apply(lambda x: x.get('average', 0))
df['num_ratings'] = df['ratings'].apply(lambda x: x.get('count', 0))
df['num_tags'] = df['tags'].apply(len)
df['genres_str'] = df['genres'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

df[['title', 'avg_rating', 'num_ratings', 'num_tags', 'genres_str']].to_csv('mongodb_movies.csv', index=False)
print("✓ Saved: mongodb_movies.csv")

client.close()

print("\n" + "=" * 80)
print("MongoDB demonstrates:")
print("  ✓ Flexible schema (varying document structures)")
print("  ✓ Nested data queries")
print("  ✓ Powerful aggregation pipelines")
print("  ✓ Array operations")
print("=" * 80)
