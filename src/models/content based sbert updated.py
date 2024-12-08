import pandas as pd
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
import warnings

warnings.filterwarnings("ignore")

# Load dataset
df = pd.read_csv("shreya.csv")
print(df.head())

# Drop duplicates
df.drop_duplicates(subset="isbn", keep="first", inplace=True)
df.drop_duplicates(subset="title", keep="first", inplace=True)

# Initialize SentenceTransformer model
sbert_model = SentenceTransformer("all-MiniLM-L6-v2")

# Compute embeddings for book descriptions
df["description_embedding"] = df["description"].apply(
    lambda x: sbert_model.encode(str(x), convert_to_numpy=True)
)

# Create FAISS index for embeddings
embedding_dim = len(df["description_embedding"].iloc[0])
faiss_index = faiss.IndexFlatL2(embedding_dim)

# Add embeddings to FAISS index
embeddings = np.vstack(df["description_embedding"].values)
faiss_index.add(embeddings)

# Save FAISS index for future use
faiss.write_index(faiss_index, "book_descriptions_faiss.index")


# Define function to recommend books based on a given title
def recommend_books_by_title(input_title, top_n=5):
    # Find the book in the dataset
    book_row = df[df["title"].str.contains(input_title, case=False, na=False)]

    if book_row.empty:
        print("Book title not found in the dataset.")
        return []

    # Get the embedding of the found book's description
    query_embedding = book_row.iloc[0]["description_embedding"]

    # Search for similar books in the FAISS index
    distances, indices = faiss_index.search(np.array([query_embedding]), top_n + 1)

    # Retrieve recommended books, excluding the input book
    recommended_books = df.iloc[indices[0][1:]]
    return recommended_books["title"].tolist()


# Example usage
input_title = "The Great Gatsby"  # Replace with your desired book title
recommendations = recommend_books_by_title(input_title, top_n=5)
print("Top 5 recommendations for '{}':".format(input_title))
print(recommendations)
