import pandas as pd
import matplotlib.pyplot as plt
from googletrans import Translator
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

df = pd.read_csv('ankita_new.csv', low_memory=False)

df.drop_duplicates(keep='first', inplace=True)

df.drop(['asin', 'edition_information'], inplace=True, axis=1)

translator = Translator()
translations_cache = {}

def translate_to_english(text):
    if not text or text in translations_cache:
        return translations_cache.get(text, text)

    try:
        detected_lang = detect(text)
        if detected_lang != 'en':
            translation = translator.translate(text, src=detected_lang, dest='en').text
            translations_cache[text] = translation
            return translation
        else:
            return text
    except LangDetectException:
        print(f"Language detection failed for: {text}")
    except Exception as e:
        print(f"Translation failed for text: {text} - Error: {e}")

    return text

df['description_translated'] = df['description'].apply(translate_to_english)


def detect_language(text):
    try:
        return detect(text)
    except LangDetectException:
        return 'unknown'

df['language_code_new'] = df['description'].apply(detect_language)

df = df.drop(['language_code', 'description'], axis=1)

df.to_csv("ankita_cleaned.csv")