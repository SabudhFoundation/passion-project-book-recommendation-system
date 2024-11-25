import pandas as pd
import numpy as np
import pymongo
import random
import json
from gensim.models import Word2Vec
import matplotlib.pyplot as plt
import warnings
import re

warnings.filterwarnings('ignore')

df = pd.read_csv('data.csv')
# Creating a synthetic user dataset
# This dataset being created will consist of the following columns: 'user_id', 'isbn', 'title', 'average_rating'

# Number of users in the synthetic dataset.
num_users = 3000

user_book_list = []

# Randomly assigning the books t each user
for user in range(1, num_users + 1):
    num_books = random.randint(30, 35)  # Assigning 30-35 books to each user.
    sampled_books = df.sample(num_books)
    for _, row in sampled_books.iterrows():
        user_book_list.append({
            "user_id": user,
            "isbn": row["isbn"],
            "title": row["title_without_series"],
            "average_rating": row['average_rating']
        })
user_book_df = pd.DataFrame(user_book_list)  # Creating the new dataset

# Saving data to MongoDB
df = pd.read_csv("data.csv")
uri = "mongodb+srv://jxxxx:83y4YltACxxxxx@cluster0.zuxxxo.mongodb.net/?retryWrxxxs=true&w=majority&appName=xxxxx"
client = pymongo.MongoClient(uri)

payload = json.loads(user_book_df.to_json(orient='records'))

database = client["RecommendationSystem"]
collection = database["book_data"]

collection.insert_many(payload)

# Retrieving data from MongoDB
data = collection.find({})
user_book_data = []
for book in data:
    user_book_data.append(book)
user_book_data = pd.DataFrame(user_book_data)  # Creating dataset from the data retrieved from MongoDB

df = user_book_data[['user_id', 'isbn', 'title', 'average_rating']]

# shuffle user ID's
num_users = 3000
users = [x for x in range(1, num_users + 1)]
random.shuffle(users, )

# extract 90% of user ID's
users_train = [users[i] for i in range(round(0.9 * len(users)))]
users_val = [users[i] for i in range(len(users)) if users[i] not in users_train]

# split data into train and validation set
train_df = df[df['user_id'].isin(users_train)]
validation_df = df[~df['user_id'].isin(users_train)]

# list to capture book history of the users
books_train = []

# populate the list with the product codes
for user in users_train:
    temp = train_df[train_df["user_id"] == user]["isbn"].tolist()
    books_train.append(temp)

# list to capture book history of the users(validation set)
books_val = []

# populate the list with the product codes
for user in users_val:
    temp = validation_df[validation_df["user_id"] == user]["isbn"].tolist()
    books_val.append(temp)

# train word2vec model
model = Word2Vec(sentences=books_train, window=5, sg=1, min_count=1, workers=4)


def recommend_items(item_id, top_n=5):
    if item_id in model.wv:
        return model.wv.most_similar(item_id, topn=top_n)
    else:
        return []


def user_embedding(user_items):
    vectors = [model.wv[item] for item in user_items if item in model.wv]
    return np.mean(vectors, axis=0) if vectors else None


def recommend_for_user(user_items, top_n=5):
    user_vec = user_embedding(user_items)
    if user_vec is None:
        return []
    return model.wv.similar_by_vector(user_vec, topn=top_n)


def preprocess_text(sen):
    # Remove punctuations and numbers
    sentence = re.sub('[^a-zA-Z]', ' ', sen)

    # Single character removal
    sentence = re.sub(r"\s+[a-zA-Z]\s+", ' ', sentence)

    # Removing multiple spaces
    sentence = re.sub(r'\s+', ' ', sentence)

    return sentence


recommendations = []
for i in range(len(users_val)):
    book_history = books_val[i]
    user_id = users_val[i]
    rec = recommend_for_user(book_history)
    titles = []
    for id, prob in rec:
        title = user_book_data[user_book_data['isbn'] == id]['title'].astype(str)
        title = preprocess_text(str(title))
        titles.append(title)

    recommendations.append({
        "user_id": user_id,
        "recommendation": rec,
        "titles": titles
    })

val_rec_df = pd.DataFrame(recommendations)

