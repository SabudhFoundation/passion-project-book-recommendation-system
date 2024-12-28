# dump final csv in mongo
import pandas as pd
from synthetic import generate_synthetic_data
from src.dbutils.connect_database import ConnectDatabase
from src.dbutils import dbwrapper

df = pd.read_csv(r"data/processed/final.csv")
df = df[:2000]

user_book_df = generate_synthetic_data(df)

db_connection = ConnectDatabase()

dbwrapper.insert_documents(db_connection, 'books_data', user_book_df, many=True)
dbwrapper.insert_documents(db_connection, 'all_books', df, many=True)
