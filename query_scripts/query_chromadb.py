import chromadb
import pandas as pd

print("=" * 80)
print("CHROMADB QUERIES - Similarity Search")
print("=" * 80)

client = chromadb.HttpClient(host='localhost', port=8000)
collection = client.get_collection("movies")

# Query 1: Find movies similar to "Toy Story"
print("\nQuery 1: Movies Similar to 'Toy Story'")
print("-" * 80)

results = collection.query(
    query_texts=["Toy Story animated adventure family comedy"],
    n_results=10
)

print("Top 10 similar movies:")
for i, (doc, metadata, distance) in enumerate(zip(results['documents'][0], 
                                                    results['metadatas'][0],
                                                    results['distances'][0])):
    print(f"{i+1}. {metadata['title']} (similarity: {1-distance:.3f})")

# Query 2: Action & Adventure recommendations
print("\nQuery 2: Action Adventure Movie Recommendations")
print("-" * 80)

results = collection.query(
    query_texts=["action packed adventure thriller explosions"],
    n_results=10
)

print("Top 10 recommendations:")
for i, (doc, metadata, distance) in enumerate(zip(results['documents'][0], 
                                                    results['metadatas'][0],
                                                    results['distances'][0])):
    print(f"{i+1}. {metadata['title']} (similarity: {1-distance:.3f})")

# Query 3: Romantic Comedy recommendations
print("\nQuery 3: Romantic Comedy Recommendations")
print("-" * 80)

results = collection.query(
    query_texts=["romantic comedy love relationship funny"],
    n_results=10
)

print("Top 10 recommendations:")
for i, (doc, metadata, distance) in enumerate(zip(results['documents'][0], 
                                                    results['metadatas'][0],
                                                    results['distances'][0])):
    print(f"{i+1}. {metadata['title']} (similarity: {1-distance:.3f})")

# Query 4: Science Fiction recommendations
print("\nQuery 4: Sci-Fi Space Movie Recommendations")
print("-" * 80)

results = collection.query(
    query_texts=["science fiction space aliens futuristic technology"],
    n_results=10
)

print("Top 10 recommendations:")
for i, (doc, metadata, distance) in enumerate(zip(results['documents'][0], 
                                                    results['metadatas'][0],
                                                    results['distances'][0])):
    print(f"{i+1}. {metadata['title']} (similarity: {1-distance:.3f})")

# Export recommendations to CSV
print("\nExporting similarity search results...")

queries = [
    ("Animated Family", "animated family children adventure"),
    ("Action Thriller", "action thriller suspense intense"),
    ("Horror Scary", "horror scary frightening dark terror"),
    ("Drama Emotional", "emotional drama moving touching sad"),
    ("Comedy Funny", "comedy funny hilarious laugh humor")
]

all_recommendations = []

for category, query in queries:
    results = collection.query(query_texts=[query], n_results=20)
    
    for title, metadata, distance in zip(results['documents'][0], 
                                         results['metadatas'][0],
                                         results['distances'][0]):
        all_recommendations.append({
            'category': category,
            'title': metadata['title'],
            'genres': metadata['genres'],
            'similarity_score': round(1 - distance, 3),
            'query': query
        })

df = pd.DataFrame(all_recommendations)
df.to_csv('chromadb_recommendations.csv', index=False)
print("✓ Saved: chromadb_recommendations.csv")

print("\n" + "=" * 80)
print("ChromaDB demonstrates:")
print("  ✓ Semantic similarity search")
print("  ✓ Natural language queries")
print("  ✓ Recommendation engine capabilities")
print("  ✓ Vector-based matching")
print("=" * 80)
