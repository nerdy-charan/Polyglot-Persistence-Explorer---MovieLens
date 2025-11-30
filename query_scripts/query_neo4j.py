from neo4j import GraphDatabase
import pandas as pd

print("=" * 80)
print("NEO4J QUERIES - Graph Relationships")
print("=" * 80)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

# Query 1: Most Active Users
print("\nQuery 1: Top 10 Most Active Users")
print("-" * 80)

with driver.session() as session:
    result = session.run("""
        MATCH (u:User)-[r:RATED]->()
        RETURN u.userId as userId, count(r) as num_ratings, avg(r.rating) as avg_rating
        ORDER BY num_ratings DESC
        LIMIT 10
    """)
    
    for record in result:
        print(f"User {record['userId']}: {record['num_ratings']} ratings (avg: {record['avg_rating']:.2f})")

# Query 2: Users who liked similar movies
print("\nQuery 2: Users Who Rated 'Toy Story' Highly Also Rated:")
print("-" * 80)

with driver.session() as session:
    result = session.run("""
        MATCH (m:Movie {title: 'Toy Story (1995)'})<-[r1:RATED {rating: 5.0}]-(u:User)
        MATCH (u)-[r2:RATED]->(m2:Movie)
        WHERE m2 <> m AND r2.rating >= 4.0
        RETURN m2.title as title, count(u) as num_users, avg(r2.rating) as avg_rating
        ORDER BY num_users DESC
        LIMIT 10
    """)
    
    for record in result:
        print(f"{record['title']}: {record['num_users']} users (avg: {record['avg_rating']:.2f})")

# Query 3: Genre Connections
print("\nQuery 3: Most Common Genre Combinations")
print("-" * 80)

with driver.session() as session:
    result = session.run("""
        MATCH (m:Movie)-[:HAS_GENRE]->(g1:Genre)
        MATCH (m)-[:HAS_GENRE]->(g2:Genre)
        WHERE g1.name < g2.name
        RETURN g1.name + ' + ' + g2.name as combo, count(m) as count
        ORDER BY count DESC
        LIMIT 10
    """)
    
    for record in result:
        print(f"{record['combo']}: {record['count']} movies")

# Query 4: Movie Recommendation Path
print("\nQuery 4: Recommendation Path (User 1 → Similar Users → Movies)")
print("-" * 80)

with driver.session() as session:
    result = session.run("""
        MATCH (u1:User {userId: 1})-[r1:RATED]->(m:Movie)<-[r2:RATED]-(u2:User)
        WHERE r1.rating >= 4.0 AND r2.rating >= 4.0 AND u1 <> u2
        WITH u1, u2, count(m) as common_movies
        WHERE common_movies >= 3
        MATCH (u2)-[r:RATED]->(rec:Movie)
        WHERE r.rating >= 4.0 AND NOT EXISTS((u1)-[:RATED]->(rec))
        RETURN rec.title as title, count(DISTINCT u2) as recommended_by, avg(r.rating) as avg_rating
        ORDER BY recommended_by DESC, avg_rating DESC
        LIMIT 10
    """)
    
    for record in result:
        print(f"{record['title']}: recommended by {record['recommended_by']} similar users (avg: {record['avg_rating']:.2f})")

# Query 5: Shortest Path Between Genres
print("\nQuery 5: Genre Network - Action to Romance Path")
print("-" * 80)

with driver.session() as session:
    result = session.run("""
        MATCH path = shortestPath((g1:Genre {name: 'Action'})-[*]-(g2:Genre {name: 'Romance'}))
        RETURN [node in nodes(path) | node.name] as path
        LIMIT 1
    """)
    
    record = result.single()
    if record and record['path']:
        path_str = " → ".join([str(p) if p else "Unknown" for p in record['path']])
        print(path_str)
    else:
        print("No direct path found between Action and Romance genres")

# Export graph data for visualization
print("\nExporting graph data...")

# User-Movie edges
with driver.session() as session:
    result = session.run("""
        MATCH (u:User)-[r:RATED]->(m:Movie)
        WHERE r.rating >= 4.0
        RETURN u.userId as userId, m.movieId as movieId, m.title as title, r.rating as rating
        LIMIT 1000
    """)
    
    df = pd.DataFrame([dict(record) for record in result])
    df.to_csv('neo4j_user_movie_edges.csv', index=False)
    print("✓ Saved: neo4j_user_movie_edges.csv")

# Genre network
with driver.session() as session:
    result = session.run("""
        MATCH (m:Movie)-[:HAS_GENRE]->(g:Genre)
        RETURN m.title as movie, g.name as genre
        LIMIT 2000
    """)
    
    df = pd.DataFrame([dict(record) for record in result])
    df.to_csv('neo4j_movie_genre_network.csv', index=False)
    print("✓ Saved: neo4j_movie_genre_network.csv")

driver.close()

print("\n" + "=" * 80)
print("Neo4j demonstrates:")
print("  ✓ Relationship traversal")
print("  ✓ Pattern matching")
print("  ✓ Collaborative filtering")
print("  ✓ Path finding algorithms")
print("=" * 80)
