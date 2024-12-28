
def recommend_book_content(df, content_vector_store, title):
    title = str(title)
    desc = df['description'][df['title'] == title].astype(str).values[0]
    content = desc + " " + title
    results_content = content_vector_store.similarity_search_with_score(
        content, k=10
    )

    recommended_books = {}
    for res, score in results_content:
        isbn = res.metadata['isbn']
        recommended_books[isbn] = recommended_books.get(isbn, 0) + 1

    rec_books = list(recommended_books.items())
    rec_books.sort(key=lambda x: x[1], reverse=True)
    rec_books = rec_books[1:6]
    isbns = [ele[0] for ele in rec_books]
    titles = [df[df['isbn'] == isbn]['title'].astype(str).values[0] for isbn in isbns]

    urls = retrieve_images(isbns)
    return urls, titles, isbns


def recommend_book_collab(id, collab_vector_store, user_book_df, df, books_data):
    id = int(id)
    results_collab = collab_vector_store.similarity_search_with_score(
        books_data[id], k=3
    )

    read = set(user_book_df[user_book_df['user_id'] == id]['isbn'].tolist())
    recommended_books = {}
    for res, score in results_collab:
        isbns = res.metadata['isbns']
        for isbn in isbns:
            if isbn not in read:
                recommended_books[isbn] = recommended_books.get(isbn, 0) + 1

    rec_books = list(recommended_books.items())
    rec_books.sort(key=lambda x: x[1], reverse=True)
    rec_books = rec_books[:5]
    isbns = [ele[0] for ele in rec_books]
    titles = [df[df['isbn'] == isbn]['title'].astype(str).values[0] for isbn in isbns]

    urls = retrieve_images(isbns)
    return urls, titles, isbns


def retrieve_images(book_ids):
    urls = []
    for book_id in book_ids:
        urls.append(df[df['isbn'] == book_id]['new_image_url'].astype(str).values[0])
    return urls
