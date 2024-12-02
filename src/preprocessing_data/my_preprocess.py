import pandas as pd
from googletrans import Translator


# Function to read the CSV file
def read_csv_file(file_path, num_rows=None):
    try:
        # Read the CSV file with optional row limit
        df = pd.read_csv(file_path, low_memory=False)

        if num_rows:
            df = df.head(num_rows)

        return df
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None


# Function to check if a column exists in the DataFrame
def check_column_exists(df, column_name):
    if column_name not in df.columns:
        print(f"Error: The '{column_name}' column is missing in the CSV file.")
        return False
    return True


# Function to translate text using Google Translator
def translate_text(text, translator):
    try:
        # Translate the text
        translated = translator.translate(text, src="auto", dest="en")
        return translated.text
    except Exception as e:
        print(f"Error translating text: {e}")
        return text  # Return the original text if translation fails


# Function to apply translation on a DataFrame column
def translate_column(df, column_name, translator):
    # Apply translation to each row in the 'description' column
    df["Translated_Text"] = df[column_name].apply(
        lambda x: translate_text(x, translator)
    )
    return df


# Function to drop a column from the DataFrame
def drop_column(df, column_name):
    df.drop(columns=[column_name], inplace=True)
    return df


# Function to rename a column in the DataFrame
def rename_column(df, old_name, new_name):
    df.rename(columns={old_name: new_name}, inplace=True)
    return df


# Function to reorder columns in the DataFrame
def reorder_columns(df, column_order):
    df = df[column_order]
    return df


# Function to save the DataFrame to a CSV file
def save_to_csv(df, file_path):
    try:
        df.to_csv(file_path, index=False)
        print(f"File saved to {file_path}")
    except Exception as e:
        print(f"Error saving the CSV file: {e}")


# Main function to orchestrate the preprocessing steps
def preprocess_csv(file_path, output_path, num_rows=1000):
    # Initialize the translator
    translator = Translator()

    # Step 1: Read the CSV file
    df = read_csv_file(file_path, num_rows)
    if df is None:
        return

    # Step 2: Check if the 'description' column exists
    if not check_column_exists(df, "description"):
        return

    # Step 3: Translate the 'description' column
    df = translate_column(df, "description", translator)

    # Step 4: Drop the 'description' column
    df = drop_column(df, "description")

    # Step 5: Rename 'Translated_Text' to 'Description'
    df = rename_column(df, "Translated_Text", "Description")

    # Step 6: Reorder columns (optional: specify the order of columns)
    df = reorder_columns(df, ["title", "Description"])

    # Step 7: Save the processed DataFrame to a new CSV file
    save_to_csv(df, output_path)



# if __name__ == "__main__":
#     input_file = "shreya.csv"  # Input file path
#     output_file = "processed_file.csv"  # Output file path
#     preprocess_csv(input_file, output_file, num_rows=1000)
