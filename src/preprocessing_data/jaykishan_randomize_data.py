import pandas as pd
import random
import numpy as np

import random
np.random.seed(3)

# This function will randomize assign books to 'num_users' users.
def randomize_data(df):
    num_users = 500
    users = [x for x in range(1, num_users + 1)]
    random.shuffle(users, )

    # extract 90% of user ID's
    users_train = [users[i] for i in range(round(0.9 * len(users)))]
    users_val = [users[i] for i in range(len(users)) if users[i] not in users_train]

    # split data into train and validation set
    train_df = df[df['user_id'].isin(users_train)]
    validation_df = df[df['user_id'].isin(users_val)]

    return users_train, users_val, train_df, validation_df



