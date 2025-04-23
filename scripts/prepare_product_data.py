import os
import random

import pandas as pd

print("Converting product data to JSON format...")

# Get the project root directory
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Define file paths with simpler names
input_file = os.path.join(project_dir, 'data', 'products', 'products_raw.csv')
output_file = os.path.join(project_dir, 'data', 'products', 'products.json')

# Ensure output directory exists
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Check if file exists
if not os.path.exists(input_file):
    print(f"Error: File not found at {input_file}")
    print("Current working directory:", os.getcwd())
    exit(1)

print(f"Reading product data from: {input_file}")
df = pd.read_csv(input_file)

# Print column names to debug
print("Available columns in the dataset:")
for col in df.columns:
    print(f"- {col}")

# Select columns dynamically based on what's available
available_columns = df.columns.tolist()
selected_columns = []

# Define column mappings (old_name: new_name)
column_mappings = {
    'product_id': 'product_id',
    'title': 'name',
    'name': 'name',  # Alternative column name
    'product_name': 'name',  # Alternative column name
    'description': 'description',
    'price': 'price',
    'retail_price': 'list_price',
    'units_sold': 'units_sold',
    'rating': 'rating',
    'product_color': 'color',
    'product_variation_size_id': 'size_id',
    'origin_country': 'country'
}

# Select available columns and create a new DataFrame
product_df = pd.DataFrame()

for old_col, new_col in column_mappings.items():
    if old_col in available_columns:
        product_df[new_col] = df[old_col]

# Ensure required columns exist
required_columns = ['product_id', 'name', 'price']
for col in required_columns:
    if col not in product_df.columns:
        if col == 'product_id' and 'id' in available_columns:
            # Use 'id' as fallback for product_id
            product_df['product_id'] = df['id']
        elif col == 'product_id':
            # Generate sequential IDs if no product_id column exists
            product_df['product_id'] = range(1, len(df) + 1)
        elif col == 'name' and len(available_columns) > 0:
            # Use the first available column as name if none found
            product_df['name'] = df[available_columns[0]]
        elif col == 'price' and 'cost' in available_columns:
            # Use 'cost' as fallback for price
            product_df['price'] = df['cost']
        else:
            # Generate placeholder data
            if col == 'price':
                product_df[col] = [random.uniform(10, 100) for _ in range(len(df))]
            else:
                product_df[col] = [f"Default {col} {i}" for i in range(len(df))]

# Fill missing optional columns with defaults
if 'description' not in product_df.columns:
    product_df['description'] = product_df['name'].apply(lambda x: f"Description for {x}")

if 'list_price' not in product_df.columns and 'price' in product_df.columns:
    product_df['list_price'] = product_df['price'] * 1.2

if 'category' not in product_df.columns:
    categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys']
    product_df['category'] = [random.choice(categories) for _ in range(len(df))]

if 'stock_quantity' not in product_df.columns:
    product_df['stock_quantity'] = [random.randint(5, 100) for _ in range(len(df))]

if 'added_date' not in product_df.columns:
    product_df['added_date'] = pd.Timestamp.now().strftime('%Y-%m-%d')

# Fill NaN values
for col in product_df.columns:
    if product_df[col].dtype == 'object':
        product_df[col] = product_df[col].fillna('')
    else:
        product_df[col] = product_df[col].fillna(0)

# Convert to JSON
products_json = product_df.to_json(orient='records', indent=2)

# Save to file
with open(output_file, 'w') as f:
    f.write(products_json)

print(f"Successfully converted {len(product_df)} products to JSON format")
print(f"Output saved to: {output_file}")