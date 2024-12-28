import pandas as pd
import numpy as np
import sys
import os
from data import synthetic
from preprocessing_data import jaykishan_randomize_data
from src.models import jaykishan_model_building
from src.models import jaykishan_recommend_book
from src.dbutils import dbwrapper, connect_database
import gradio as gr

# Add the directory containing `helper.py` to sys.path
sys.path.append(os.path.abspath(r"data"))
sys.path.append(os.path.abspath(r"data/preprocessing_data"))

db_connection = connect_database.ConnectDatabase()

user_book_df = dbwrapper.fetch_documents(db_connection, 'books_data')
df = dbwrapper.fetch_documents(db_connection, 'all_books')

users_train, users_val, train_df, validation_df = jaykishan_randomize_data.randomize_data(user_book_df)

collab_vector_store, content_vector_store, books_data = jaykishan_model_building.create_recommendation_model(df,
                                                                                                             users_train,
                                                                                                             users_val,
                                                                                                             train_df,
                                                                                                             validation_df)


def get_info(evt: gr.SelectData):
    global info
    return info[evt.index]


def get_info_content(evt: gr.SelectData):
    global info_content
    return info_content[evt.index]


def recommend_collab(id):
    urls, titles, isbns = jaykishan_recommend_book.recommend_book_collab(id, collab_vector_store, user_book_df, df,
                                                                         books_data)

    global info
    info = {}
    k = 0
    for book_id in isbns:
        temp = df[df['isbn'] == book_id]
        info[k] = {
            "Title": temp['title'].astype(str).values[0],
            "Author": temp['authors'].astype(str).values[0],
            "Description": temp['description'].astype(str).values[0],
        }
        k += 1

    return zip(urls, titles)


def recommend_content(title):
    urls, titles, isbns = jaykishan_recommend_book.recommend_book_content(df, content_vector_store, title)

    global info_content
    info_content = {}
    k = 0
    for book_id in isbns:
        temp = df[df['isbn'] == book_id]
        info_content[k] = {
            "Title": temp['title'].astype(str).values[0],
            "Author": temp['authors'].astype(str).values[0],
            "Description": temp['description'].astype(str).values[0],
        }
        k += 1

    return zip(urls, titles)


with gr.Blocks() as demo:
    with gr.Row():
        with gr.Column():
            gallery = gr.Gallery(
                label="Generated images", show_label=False, elem_id="gallery"
                , columns=[5], rows=[1], object_fit="contain", height="auto")
            info_box = gr.JSON()
            gallery.change(get_info, gallery, info_box)
            gallery.select(fn=get_info, outputs=info_box)
            btn = gr.Button("Generate Book Recommendations", scale=0)
            btn.click(recommend_collab, gr.Slider(1, 500, step=1, label="User ID"), gallery)

            gallery.select(fn=get_info, outputs=info_box)

        with gr.Column():
            gallery2 = gr.Gallery(
                label="Generated images", show_label=False, elem_id="gallery2"
                , columns=[5], rows=[1], object_fit="contain", height="auto")
            info_box2 = gr.JSON()
            gallery2.change(get_info_content, gallery2, info_box2)
            gallery2.select(fn=get_info_content, outputs=info_box2)
            btn = gr.Button("Generate Book Recommendations", scale=0)
            btn.click(recommend_content, gr.Textbox(label="Book Title", placeholder="Enter book title"), gallery2)

if __name__ == '__main__':
    demo.launch(share=True, debug=True)
