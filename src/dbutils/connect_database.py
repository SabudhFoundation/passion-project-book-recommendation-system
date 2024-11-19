import os
from pymongo import MongoClient
from dotenv import load_env
load_env()

class ConnectDatabase:
    """
    Manages the connection to a MongoDB database, including establishing, checking, 
    reconnecting, and closing the connection.

    Attributes:
        connection (MongoClient): The MongoDB client connection instance.
        database (Database): The database instance for the connected MongoDB database.

    Methods:
        connect_to_database(): Establishes a connection to the MongoDB database based on the specified URI.
        close_connection(): Closes the active MongoDB connection, if any.
        reconnect(): Re-establishes the connection to the MongoDB server by closing and reopening the connection.
        check_connection(): Checks if the current MongoDB connection is active and retrieves server information.
    """

    def __init__(self):
        """Initializes the ConnectDatabase instance and connects to the database."""
        self.connection = None
        self.database = None
        self.connect_to_database()

    def connect_to_database(self):
        """Establish a connection to the MongoDB server."""
        self.connection = MongoClient(os.getenv("MONGO_URI"))
        self.database = self.connection[os.getenv("DATABASE")]

    def close_connection(self):
        """Close the connection to the MongoDB server."""
        if self.connection:
            self.connection.close()
            self.connection = None  # Ensure the connection is not reused

    def reconnect(self):
        """Re-establish the connection to the MongoDB server."""
        self.close_connection()  # Close the existing connection if it exists
        self.connect_to_database()  # Create a new MongoClient instance

    def check_connection(self):
        """
        Checks if the MongoDB connection is active by retrieving server information.
        
        Returns:
            bool: True if the connection is active, False otherwise.

        Raises:
            Exception: If unable to retrieve server information, indicating a connection issue.
        """
        try:
            # Try to retrieve server information
            server_info = self.connection.server_info()
            print("MongoDB connection is active.")
            print("Server Info:", server_info)
            return True
        
        except Exception as e:
            print(f"MongoDB connection is not active: {e}")
            return False