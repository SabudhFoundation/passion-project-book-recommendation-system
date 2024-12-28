# put the function of synthetic data generation and store synthethic data in mongo
import pandas as pd
import numpy as np


def generate_synthetic_data(df):
    # Creating a synthetic user dataset
    num_users = 500

    user_book_list = []
    u_ids = [i for i in range(1, 1 + num_users)]
    for i in range(len(df)):
        n_users = int(50 + (6.5) * np.random.standard_normal())
        reader_ids = np.random.choice(u_ids, n_users, replace=True)
        row = df.loc[i]
        for reader in reader_ids:
            user_r = row['average_rating'].astype(float)
            r_scale = (5 - user_r) / 3
            user_rating = int(np.random.normal(loc=user_r, scale=r_scale))
            user_book_list.append({
                "user_id": reader,
                "isbn": row["isbn"],
                "title": row["title"],
                "rating": user_rating,
                "description": row['description'],
                "image_url": row['new_image_url'],
            })

    user_book_df = pd.DataFrame(user_book_list)

    return user_book_df
