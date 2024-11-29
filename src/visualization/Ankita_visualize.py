import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Importing the data
df = pd.read_csv('ankita_new.csv', low_memory=False)

# Fixing the data types of required columns from the DataFrame
df['publication_day'] = pd.to_numeric(df['publication_day'], errors='coerce').astype('Int64')
df['publication_month'] = pd.to_numeric(df['publication_month'], errors='coerce').astype('Int64')
df['publication_year'] = pd.to_numeric(df['publication_year'], errors='coerce').astype('Int64')

df['isbn'] = pd.to_numeric(df['isbn'], errors='coerce')
df['isbn13'] = pd.to_numeric(df['isbn13'], errors='coerce')

df['asin'] = df['asin'].fillna('').astype(str)
df['book_id'] = df['book_id'].fillna('').astype(str)

df['num_pages'] = pd.to_numeric(df['num_pages'], errors='coerce').astype('Int64')
df['ratings_count'] = pd.to_numeric(df['ratings_count'], errors='coerce').astype('Int64')

df['work_id'] = df['work_id'].fillna('').astype(str)

# Selecting all numerical columns from the DataFrame
num_col = df.select_dtypes(include=['number'])

# Calculating the total number of missing values in each column of the DataFrame
missing_values = df.isnull().sum()

# Barplot of count of missing values in each column of the DataFrame
missing_values_filtered = missing_values[missing_values > 0]
plt.figure(figsize=(10, 6))
missing_values_filtered.plot(kind='bar', color='skyblue')
plt.title('Count of Missing Values per Column')
plt.ylabel('Number of Missing Values')
plt.xticks(rotation=45)
plt.show()

# Histogram of the numerical columns of the DataFrame
num_col.hist(figsize=(15, 8), bins=10, color='skyblue', edgecolor='black')
plt.suptitle('Histograms of Numerical Columns', fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.show()

# Boxplot of the numerical columns of the DataFrame
plt.figure(figsize=(15, 8))
num_cols = num_col.shape[1]
nrows = (num_cols // 3) + (num_cols % 3 > 0)

for i, column in enumerate(num_col.columns, 1):
    plt.subplot(nrows, 3, i)
    sns.boxplot(data=num_col[column], color='skyblue')
    plt.title(column)
    plt.xlabel('Value')

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.suptitle('Box Plots of Numerical Columns', fontsize=16)
plt.show()

# Correlation Matrix of the numerical columns of the DataFrame
correlation_matrix = num_col.corr()
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
plt.title('Correlation Matrix for Numerical Columns')
plt.show()

# Scatterplot of reviews and average_rating from the DataFrame
plt.figure(figsize=(10, 6))
sns.scatterplot(x='text_reviews_count', y='average_rating', data=df)
plt.xlabel('Number of Text Reviews')
plt.ylabel('Average Rating')
plt.title('Relationship between Number of Reviews and Average Rating')
plt.show()

import ast
from collections import Counter

def extract_shelves(shelves_entry):
    try:
        shelves = ast.literal_eval(shelves_entry)
        return [(shelf['name'], int(shelf['count'])) for shelf in shelves]
    except (ValueError, SyntaxError):
        return []

df['shelves_list'] = df['popular_shelves'].apply(extract_shelves)

all_shelves = [(shelf, count) for sublist in df['shelves_list'] for shelf, count in sublist]

shelf_counter = Counter()
for shelf, count in all_shelves:
    shelf_counter[shelf] += count

shelf_counts_df = pd.DataFrame(shelf_counter.items(), columns=['Shelf', 'Total Count']).sort_values(by='Total Count', ascending=False)

# Barplot of most popular shelves in the popular_shelves columns in the DataFrame
plt.figure(figsize=(10, 6))
sns.barplot(x='Total Count', y='Shelf', data=shelf_counts_df.head(10), palette='coolwarm')
plt.xlabel('Total Count')
plt.ylabel('Shelf Name')
plt.title('Top 10 Most Popular Shelves')
plt.show()