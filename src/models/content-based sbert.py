import pandas as pd
import numpy as np
import random
import re
from tqdm import tqdm
import faiss
from sentence_transformers import SentenceTransformer
import warnings

warnings.filterwarnings("ignore")

# Load dataset
df = pd.read_csv("shreya.csv")
print(df.head())
print(df.info())
print(df.isna().sum())

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

# Create a synthetic user dataset
num_users = 3000
user_book_list = []

for user in range(1, num_users + 1):
    num_books = random.randint(30, 35)
    sampled_books = df.sample(num_books)
    for _, row in sampled_books.iterrows():
        user_book_list.append(
            {
                "user_id": user,
                "isbn": row["isbn"],
                "title": row["title_without_series"],
                "average_rating": row["average_rating"],
                "description": row["description"],
            }
        )

user_book_df = pd.DataFrame(user_book_list)
print(user_book_df.head())

# Save user-book dataset
user_book_df.to_csv("user_book_dataset2.csv", index=False)

# Shuffle user IDs and create train/validation split
users = [x for x in range(1, num_users + 1)]
random.shuffle(users)

# Extract 90% of customer IDs
users_train = [users[i] for i in range(round(0.9 * len(users)))]
users_val = [users[i] for i in range(len(users)) if users[i] not in users_train]

# Split data into train and validation sets
train_df = user_book_df[user_book_df["user_id"].isin(users_train)]
validation_df = user_book_df[~user_book_df["user_id"].isin(users_train)]


# Define recommendation function using FAISS
def recommend_items_faiss(query_embedding, top_n=5):
    distances, indices = faiss_index.search(np.array([query_embedding]), top_n)
    recommended_books = df.iloc[indices[0]]
    return recommended_books


# Generate recommendations for validation set
recommendations = []
for i in tqdm(range(len(validation_df["user_id"].unique()))):
    user_id = validation_df["user_id"].unique()[i]
    user_books = validation_df[validation_df["user_id"] == user_id]

    # Combine embeddings of books the user has interacted with
    user_embedding = np.mean(
        [
            sbert_model.encode(str(desc), convert_to_numpy=True)
            for desc in user_books["description"]
        ],
        axis=0,
    )

    # Get recommendations
    recommended_books = recommend_items_faiss(user_embedding, top_n=5)
    recommendations.append(
        {"user_id": user_id, "titles": recommended_books["title"].tolist()}
    )

# Convert recommendations to DataFrame
val_rec_df = pd.DataFrame(recommendations)

# Save recommendations to CSV
val_rec_df.to_csv("validation_recommendations.csv", index=False)

print("Recommendations saved to validation_recommendations.csv")
