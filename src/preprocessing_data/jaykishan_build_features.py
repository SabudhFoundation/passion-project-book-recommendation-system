# all featuring engineering functions put in this script like remove stopwords, lemitization, translation and cleaning data
import pandas as pd
import numpy as np
from googletrans import Translator
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException


def build_fearures(df):
    # Removing duplicates
    df.drop_duplicates(inplace=True)

    translator = Translator()
    translations_cache = {}  # Cache for storing previously translated texts

    # Translate non-english description to english
    def translate_to_english(text):
        if not text or text in translations_cache:
            return translations_cache.get(text, text)

        try:
            # Detect language and translate if necessary
            detected_lang = detect(text)
            if detected_lang != 'en':
                translation = translator.translate(text, src=detected_lang, dest='en').text
                translations_cache[text] = translation  # Cache the translation
                return translation
            else:
                return text
        except LangDetectException:
            print(f"Language detection failed for: {text}")
        except Exception as e:
            print(f"Translation failed for text: {text} - Error: {e}")

        return text

    # Function to detect language_code
    def detect_language(text):
        try:
            return detect(text)
        except LangDetectException:
            return 'unknown'

    df['description'] = df['description'].apply(translate_to_english)  # Translate description
    df['language_code_new'] = df['description'].apply(detect_language)  # Identify language_code

    # Correcting the datatypes and filling up the missing values
    df['book_id'] = df['book_id'].fillna(0).astype(int)
    df['publication_year'] = df['publication_year'].fillna(0).astype(int)
    df['publication_month'] = df['publication_month'].fillna(0).astype(int)
    df['average_rating'] = df['average_rating'].astype(float)
    df['ratings_count'] = df['ratings_count'].fillna(0).astype(int)
    df['language_code'] = df['language_code'].fillna('unknown').astype(str)
    df['country_code'] = df['country_code'].fillna('unknown').astype(str)
    df['num_pages'] = df['num_pages'].fillna(df['num_pages'].median())
    df['publisher'] = df['publisher'].fillna('unknown')
    df['text_reviews_count'] = df['text_reviews_count'].fillna(0).astype(int)

    df.drop_duplicates(subset='isbn', keep='first', inplace=True)
    df.drop_duplicates(subset='title', keep='first', inplace=True)

    # Selecting all the relevant columns.
    data = df[['isbn', 'title', 'description', 'average_rating', 'language_code', 'image_url', 'authors']]
    # Save the csv file
    return data
