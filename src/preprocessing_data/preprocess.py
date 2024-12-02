import pymongo
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from googletrans import Translator

# Download necessary NLTK resources (if you haven't already)
nltk.download("punkt")
nltk.download("stopwords")
nltk.download("wordnet")


class MongoDBProcessor:
    def __init__(
        self,
        db_uri="mongodb://localhost:27017/",
        db_name="AI_for_book_analysis",
        collection_name="Books",
    ):
        # Initialize MongoDB connection
        self.client = pymongo.MongoClient(db_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

        # Initialize stop words, lemmatizer, and translator
        self.custom_stop_words = self.get_custom_stop_words()
        self.stop_words = self.custom_stop_words.union(set(stopwords.words("english")))
        self.lemmatizer = WordNetLemmatizer()
        self.translator = Translator()  # Initialize the Google Translate API

    def get_custom_stop_words(self):
        # Create a custom stop word list (you can modify this list)
        return set(
            [
                "the",
                "a",
                "an",
                "and",
                "is",
                "it",
                "of",
                "to",
                "in",
                "on",
                "for",
                "by",
                "with",
                "as",
                "that",
                "was",
                "at",
                "this",
                "which",
                "or",
                "from",
                "be",
                "are",
                "not",
                "have",
                "but",
                "all",
                "we",
                "our",
                "your",
                "you",
                "they",
                "their",
                "had",
                "has",
                "will",
                "can",
                "do",
                "does",
                "did",
                "just",
                "if",
                "how",
                "so",
                "about",
            ]
        )

    def clean_and_lemmatize_description(self, text):
        """
        Tokenizes the input text, removes stopwords, and lemmatizes the remaining words.
        """
        words = word_tokenize(text.lower())  # Tokenize text and convert to lowercase
        lemmatized_text = " ".join(
            [
                self.lemmatizer.lemmatize(word)
                for word in words
                if word not in self.stop_words and word.isalpha()
            ]
        )
        return lemmatized_text

    def translate_text(self, text):
        """
        Translates the text to English using Google Translate.
        """
        try:
            # Translate the text to English (auto-detect the source language)
            translated = self.translator.translate(text, src="auto", dest="en")
            return translated.text
        except Exception as e:
            print(f"Error translating text: {e}")
            return text  # Return original text if translation fails

    def fetch_documents(self):
        """
        Fetches documents from MongoDB collection.
        """
        return self.collection.find()

    def update_description(self, doc_id, new_description):
        """
        Updates the 'description' field in MongoDB document with the new description.
        """
        self.collection.update_one(
            {"_id": doc_id}, {"$set": {"description": new_description}}
        )

    def process_documents(self):
        """
        Fetches documents, translates, cleans, lemmatizes the descriptions, and updates them in MongoDB.
        """
        documents = self.fetch_documents()

        for doc in documents:
            description = doc.get("description", "")
            if description:
                # Translate the description to English
                translated_description = self.translate_text(description)
                # Clean and lemmatize the translated description
                cleaned_and_lemmatized_description = (
                    self.clean_and_lemmatize_description(translated_description)
                )
                # Update the description in MongoDB
                self.update_description(doc["_id"], cleaned_and_lemmatized_description)
                print(f"Updated description for book with _id: {doc['_id']}")


# # Main function to run the preprocessing script
# def main():
#     processor = MongoDBProcessor()  # Initialize the MongoDBProcessor
#     processor.process_documents()  # Process the documents

#     print(
#         "Description translation, cleaning, and lemmatization completed for all books in the collection."
#     )


# # Run the main function
# if __name__ == "__main__":
#     main()
