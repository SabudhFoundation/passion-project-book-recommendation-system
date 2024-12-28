import pandas as pd
import numpy as np
import faiss
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document


def create_recommendation_model(df, users_train, users_val, train_df, validation_df):
    # list to capture book history of the users
    books_train = []
    books_train_isbns = []

    # populate the list with the book history
    for user in users_train:
        temp = train_df[train_df["user_id"] == user][["isbn", "title", "description", "user_id", "rating"]]
        temp["combined"] = train_df.apply(
            lambda x: str(x["user_id"]) + str(x["description"]) + str(x["title"]) + str(x['rating']), axis=1)
        books_train_isbns.append(temp["isbn"].tolist())
        books_train.append(temp["combined"].tolist())

    # list to capture book history of the users(validation set)
    books_val = []
    books_val_isbns = []

    # populate the list with the product codes
    for user in users_val:
        temp = validation_df[validation_df["user_id"] == user][["isbn", "title", "description", "user_id", "rating"]]
        temp["combined"] = validation_df.apply(
            lambda x: str(x["user_id"]) + str(x["description"]) + str(x["title"]) + str(x['rating']), axis=1)
        books_val_isbns.append(temp["isbn"].tolist())
        books_val.append(temp["combined"].tolist())

    model_name = "all-MiniLM-L6-v2"

    embeddings = HuggingFaceEmbeddings(model_name=model_name)

    index = faiss.IndexFlatIP(len(embeddings.embed_query("Hello World")))

    collab_vector_store = FAISS(
        embedding_function=embeddings,
        index=index,
        docstore=InMemoryDocstore(),
        index_to_docstore_id={},
    )

    content_vector_store = FAISS(
        embedding_function=embeddings,
        index=index,
        docstore=InMemoryDocstore(),
        index_to_docstore_id={},
    )

    documents = []
    idx = 0

    def create_document(book_history):
        global idx
        book_codes = " ".join(book_history)
        book_isbns = books_train_isbns[idx]
        idx += 1
        return Document(page_content=book_codes, metadata={"isbns": book_isbns})

    for book_history in books_train:
        doc = create_document(book_history)
        documents.append(doc)

    ids = [str(user) for user in users_train]
    collab_vector_store.add_documents(documents=documents, ids=ids)

    content_docs = []

    def create_content_doc(desc, title, isbn):
        return Document(page_content=desc, metadata={"title": title, "isbn": isbn})

    for i in range(len(df)):
        temp = df.loc[i]
        desc = temp['description']
        title, isbn = temp['title'], temp['isbn']
        doc = create_content_doc(desc, title, isbn)
        content_docs.append(doc)

    ids = df['isbn'].tolist()
    content_vector_store.add_documents(documents=content_docs, ids=ids)

    collab_vector_store.save_local("collab-store")
    content_vector_store.save_local("content-store")

    books_data = {}

    def generate_data():
        for i in range(len(users_train)):
            books_data[users_train[i]] = " ".join(books_train[i])

        for i in range(len(users_val)):
            books_data[users_val[i]] = " ".join(books_val[i])

        return books_data
    books_data = generate_data()


    return collab_vector_store, content_vector_store, books_data