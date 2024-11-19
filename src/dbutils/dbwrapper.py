import datetime
import logging
import os
import traceback
from bson import ObjectId
from connect_database import ConnectDatabase

db_connection = ConnectDatabase()

def insert_documents(collection_name, documents_to_insert, many=None):
    """
    Inserts documents into a specified MongoDB collection with automatic retries in case of failure.

    This function attempts to insert documents into a MongoDB collection. It supports inserting either a 
    single document or multiple documents (depending on the `many` parameter). If the connection to the 
    database fails, it will retry the operation up to 5 times with an exponentially increasing wait time 
    between retries (starting from 5 seconds, up to a maximum of 40 seconds). If the insertion is successful, 
    it returns the inserted document IDs.

    Args:
        collection_name (str): The name of the MongoDB collection where documents should be inserted.
        documents_to_insert (dict or list): A document or list of documents to be inserted into the collection.
        many (bool, optional): A flag indicating whether to insert multiple documents (`True`) or a single document (`False`).
                                Defaults to `None`, which inserts a single document if not provided.

    Returns:
        list or str: The inserted document ID(s). If multiple documents are inserted, a list of inserted IDs is returned.
                     Otherwise, a single document ID is returned.

    Raises:
        Exception: If the insertion fails after the maximum number of retry attempts or due to other errors.

    """
    try:
        if db_connection.check_connection():
            collection = db_connection.database[collection_name]
            # condition used for documents insert using inser_many fuction
            if many:
                inserted_docs = collection.insert_many(documents_to_insert)
                id = inserted_docs.inserted_ids
                return id
            else:
                inserted_docs = collection.insert_one(documents_to_insert)
                id = inserted_docs.inserted_id
                return id
        else:
            print("retry")
            logging.info("retry")
            db_connection.reconnect()
            collection = db_connection.database[collection_name]
            # condition used for documents insert using inser_many fuction
            if many:
                inserted_docs = collection.insert_many(documents_to_insert)
                id = inserted_docs.inserted_ids
                return id
            else:
                inserted_docs = collection.insert_one(documents_to_insert)
                id = inserted_docs.inserted_id
                return id
        # print list of the _id values of the inserted documents:

    # Handle exceptions:
    except Exception as e:
        raise Exception(e)


def fetch_documents(
    collection_name, limit_count=None,condition=None, one=None, columns=None, sort_condition=None,distinct= None,key=None
):
    """
    Fetches documents from a specified MongoDB collection with automatic retries in case of failure.

    This function allows fetching documents from a MongoDB collection with flexible query options. It supports 
    querying based on conditions, sorting, limiting the number of results, selecting specific fields (columns), 
    and retrieving distinct values. If the connection to the database fails, it will retry the operation up to 
    5 times with an exponentially increasing wait time between retries (starting from 5 seconds, up to a maximum 
    of 40 seconds). If the fetch is successful, it returns the requested documents.

    Args:
        collection_name (str): The name of the MongoDB collection from which documents should be fetched.
        limit_count (int, optional): The maximum number of documents to return. If not specified, all matching documents are returned.
        condition (dict, optional): A filter condition to apply to the documents being fetched. Defaults to `None`.
        one (bool, optional): If `True`, fetches a single document that matches the condition. Defaults to `None`.
        columns (dict, optional): A projection that specifies which fields to include or exclude in the returned documents. Defaults to `None`.
        sort_condition (tuple, optional): A tuple `(field, direction)` to sort the documents by. The direction is either 1 (ascending) or -1 (descending). Defaults to `None`.
        distinct (bool, optional): If `True`, retrieves distinct values of a specific field. Defaults to `None`.
        key (str, optional): The field to retrieve distinct values from. Required if `distinct` is `True`.

    Returns:
        list or dict: If `distinct` is specified, a list of distinct values is returned. If `one` is specified, a single document is returned.
                      Otherwise, a list of documents matching the conditions is returned.

    Raises:
        Exception: If the fetch operation fails after the maximum number of retry attempts or due to other errors.

    Workflow:
        1. Checks the database connection using `db_connection.check_connection()`.
        2. If the connection is successful, fetches the documents based on the provided parameters.
        3. If the connection fails, attempts to reconnect and retries the fetch operation.
        4. If the fetch operation is successful, returns the requested documents or values.
        5. In case of any exception, the function raises the error.


    """
    try:
        if db_connection.check_connection():
            collection = db_connection.database[collection_name]
            # condition used for documents fetch using condition or not
            if one:
                if not sort_condition:
                    if condition and columns:
                        documents = collection.find_one(condition, columns)
                        return documents
                    elif condition:
                        documents = collection.find_one(condition)
                        return documents
                else:
                    if condition and columns:
                        documents = collection.find_one(filter=condition,sort=[(sort_condition[0],sort_condition[1])],projection=columns)
                        return documents
                    elif condition:
                        documents = collection.find_one(filter=condition,sort=[(sort_condition[0],sort_condition[1])])
                        return documents
            elif distinct:
                if condition and key:
                    documents = collection.distinct(key,condition)
                    return documents
                elif key:
                    documents = collection.distinct(key)
                    return documents
            else:
                if limit_count:
                    if sort_condition:
                        if condition and columns:
                            documents = collection.find(condition, columns).sort( sort_condition[0], sort_condition[1]).limit(limit_count)
                            return documents
                        elif condition:
                            documents = collection.find(condition).sort( sort_condition[0], sort_condition[1]).limit(limit_count)
                            return documents
                        elif columns:
                            documents = collection.find(projection=columns).sort( sort_condition[0], sort_condition[1]).limit(limit_count)
                            return documents
                        else:
                            documents = collection.find().sort( sort_condition[0], sort_condition[1]).limit(limit_count)
                            return documents
                    else:
                        if condition and columns:
                            documents = collection.find(condition, columns).limit(limit_count)
                            return documents
                        elif condition:
                            documents = collection.find(condition).limit(limit_count)
                            return documents
                        elif columns:
                            documents = collection.find(projection=columns).limit(limit_count)
                            return documents
                        else:
                            documents = collection.find().limit(limit_count)
                            return documents
                else:
                    if sort_condition:
                        if condition and columns:
                            documents = collection.find(condition, columns).sort( sort_condition[0], sort_condition[1])
                            return documents
                        elif condition:
                            documents = collection.find(condition).sort( sort_condition[0], sort_condition[1])
                            return documents
                        elif columns:
                            documents = collection.find(projection=columns).sort( sort_condition[0], sort_condition[1])
                            return documents
                        else:
                            documents = collection.find().sort( sort_condition[0], sort_condition[1])
                            return documents
                    else:
                        if condition and columns:
                            documents = collection.find(condition, columns)
                            return documents
                        elif condition:
                            documents = collection.find(condition)
                            return documents
                        elif columns:
                            documents = collection.find(projection=columns)
                            return documents
                        else:
                            documents = collection.find()
                            return documents
                
        else:
            print("retry")
            logging.info("retry")
            db_connection.reconnect()
            collection = db_connection.database[collection_name]
            # condition used for documents fetch using condition or not
            if one:
                if not sort_condition:
                    if condition and columns:
                        documents = collection.find_one(condition, columns)
                        return documents
                    elif condition:
                        documents = collection.find_one(condition)
                        return documents
                else:
                    if condition and columns:
                        documents = collection.find_one(filter=condition,sort=[(sort_condition[0],sort_condition[1])],projection=columns)
                        return documents
                    elif condition:
                        documents = collection.find_one(filter=condition,sort=[(sort_condition[0],sort_condition[1])])
                        return documents
            elif distinct:
                if condition and key:
                    documents = collection.distinct(key,condition)
                    return documents
                elif key:
                    documents = collection.distinct(key)
                    return documents
            else:
                if limit_count:
                    if sort_condition:
                        if condition and columns:
                            documents = collection.find(condition, columns).sort( sort_condition[0], sort_condition[1]).limit(limit_count)
                            return documents
                        elif condition:
                            documents = collection.find(condition).sort( sort_condition[0], sort_condition[1]).limit(limit_count)
                            return documents
                        elif columns:
                            documents = collection.find(projection=columns).sort( sort_condition[0], sort_condition[1]).limit(limit_count)
                            return documents
                        else:
                            documents = collection.find().sort( sort_condition[0], sort_condition[1]).limit(limit_count)
                            return documents
                    else:
                        if condition and columns:
                            documents = collection.find(condition, columns).limit(limit_count)
                            return documents
                        elif condition:
                            documents = collection.find(condition).limit(limit_count)
                            return documents
                        elif columns:
                            documents = collection.find(projection=columns).limit(limit_count)
                            return documents
                        else:
                            documents = collection.find().limit(limit_count)
                            return documents
                else:
                    if sort_condition:
                        if condition and columns:
                            documents = collection.find(condition, columns).sort( sort_condition[0], sort_condition[1])
                            return documents
                        elif condition:
                            documents = collection.find(condition).sort( sort_condition[0], sort_condition[1])
                            return documents
                        elif columns:
                            documents = collection.find(projection=columns).sort( sort_condition[0], sort_condition[1])
                            return documents
                        else:
                            documents = collection.find().sort( sort_condition[0], sort_condition[1])
                            return documents
                    else:
                        if condition and columns:
                            documents = collection.find(condition, columns)
                            return documents
                        elif condition:
                            documents = collection.find(condition)
                            return documents
                        elif columns:
                            documents = collection.find(projection=columns)
                            return documents
                        else:
                            documents = collection.find()
                            return documents
           
    # Handle exceptions:
    except Exception as e:
        print(e)
        raise Exception(e)


def update_docs(collection_name, condition, update_query, upsert=None, many=None):
    """
    Updates documents in a specified MongoDB collection based on a given condition and update query, with retry functionality.

    This function updates one or more documents in a MongoDB collection. It allows for conditional updates, 
    bulk updates, and upsert operations (insert if not found). If a database connection failure occurs, 
    the function will retry the update operation up to 5 times with exponential backoff (from 5 to 40 seconds).

    Args:
        collection_name (str): The name of the MongoDB collection where the update is to be applied.
        condition (dict): The filter condition used to select the documents for updating.
        update_query (dict): The update query that defines the modifications to apply to the matching documents.
        upsert (bool, optional): If `True`, inserts a new document if no document matches the condition. Defaults to `None`.
        many (bool, optional): If `True`, updates multiple documents that match the condition. If `False`, updates only the first matching document. Defaults to `None`.

    Returns:
        bool: `True` if the update operation is successfully performed.

    Raises:
        Exception: Raises an exception if the update operation fails after all retries.

    Workflow:
        1. Checks if there is an active database connection using `db_connection.check_connection()`.
        2. If connected, it performs the update operation based on the provided parameters (`upsert` and `many`).
            - If `many` is `True`, `update_many` is used to update all matching documents.
            - If `many` is `False` (or not specified), `update_one` is used to update the first matching document.
            - If `upsert` is `True`, inserts a new document if none match the specified condition.
        3. If the connection is not established, it retries the connection and reattempts the update.
        4. Upon success, it returns `True`. In case of failure, it raises an exception.

    """
    try:
        if db_connection.check_connection():
            if many:
                if upsert:
                    collection = db_connection.database[collection_name]
                    updated_docs = collection.update_many(
                        condition, update_query, upsert=upsert
                    )
                    return True
                else:
                    collection = db_connection.database[collection_name]
                    updated_docs = collection.update_many(condition, update_query)
                    return True
            else:
                if upsert:
                    collection = db_connection.database[collection_name]
                    updated_docs = collection.update_one(
                        condition, update_query, upsert=upsert
                    )
                    return True
                else:
                    collection = db_connection.database[collection_name]
                    updated_docs = collection.update_one(condition, update_query)
                    return True
        else:
            print("retry")
            logging.info("retry")
            db_connection.reconnect()
            if many:
                if upsert:
                    collection = db_connection.database[collection_name]
                    updated_docs = collection.update_many(
                        condition, update_query, upsert=upsert
                    )
                    return True
                else:
                    collection = db_connection.database[collection_name]
                    updated_docs = collection.update_many(condition, update_query)
                    return True
            else:
                if upsert:
                    collection = db_connection.database[collection_name]
                    updated_docs = collection.update_one(
                        condition, update_query, upsert=upsert
                    )
                    return True
                else:
                    collection = db_connection.database[collection_name]
                    updated_docs = collection.update_one(condition, update_query)
                    return True
    
    
    # Handle exceptions:
    except Exception as e:

        raise Exception(e)

def get_document_count(collection_name, condition={}):
    """
    Retrieves the count of documents in a specified MongoDB collection that match a given condition, with retry functionality.

    This function counts the number of documents in a MongoDB collection based on the specified filter condition.
    If the database connection fails, it will retry up to 5 times with exponential backoff (waiting between 5 and 40 seconds).

    Args:
        collection_name (str): The name of the MongoDB collection in which to count documents.
        condition (dict, optional): A dictionary defining the filter condition to match documents. 
                                    Defaults to an empty dictionary, which matches all documents.

    Returns:
        int: The count of documents that match the specified condition.

    Raises:
        Exception: Raises an exception if the count operation fails after all retries.

    Workflow:
        1. Verifies the database connection using `db_connection.check_connection()`.
        2. If connected, retrieves the count of documents that match the specified `condition`.
        3. If not connected, retries by reconnecting to the database and then reattempting the count.
        4. Returns the document count if successful.


    """
    try:
        if db_connection.check_connection():
            collection = db_connection.database[collection_name]
            count = collection.count_documents(condition)
            return count
        else:
            print("retry")
            logging.info("retry")
            db_connection.reconnect()
            collection = db_connection.database[collection_name]
            count = collection.count_documents(condition)
            return count
    # Handle exceptions:
    except Exception as e:

        raise Exception(e)

