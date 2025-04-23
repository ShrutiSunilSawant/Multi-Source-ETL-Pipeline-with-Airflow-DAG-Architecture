import os
import sqlite3

import pandas as pd
from sqlalchemy import create_engine

print("Setting up order database...")

# Get the project root directory
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Define the path to the order data file
order_file = os.path.join(project_dir, 'data', 'orders', 'orders.csv')

# Define the path for the SQLite database
db_file = os.path.join(project_dir, 'data', 'ecommerce.db')

# Check if file exists
if not os.path.exists(order_file):
    print(f"Error: Order data file not found: {order_file}")
    print("Current working directory:", os.getcwd())
    exit(1)

print(f"Loading order data from: {order_file}")
print("This may take a few minutes for large files...")

# Try different encodings for CSV file
encodings = ['utf-8', 'latin-1', 'cp1252', 'ISO-8859-1']
for encoding in encodings:
    try:
        print(f"Trying to read CSV with {encoding} encoding...")
        retail_data = pd.read_csv(order_file, encoding=encoding)
        print(f"Successfully loaded with {encoding} encoding")
        break
    except UnicodeDecodeError:
        print(f"Failed with {encoding} encoding, trying next...")
        continue
else:
    print("Failed to read file with any encoding. Please check the file format.")
    exit(1)

# Data cleanup
print("Cleaning and preparing order data...")
print("Column names in the dataset:", retail_data.columns.tolist())

# Check if the required columns exist
required_columns = ['InvoiceNo', 'CustomerID', 'InvoiceDate', 'Quantity', 'UnitPrice']
missing_columns = [col for col in required_columns if col not in retail_data.columns]

if missing_columns:
    print(f"Warning: Missing required columns: {missing_columns}")
    print("Available columns:", retail_data.columns.tolist())
    exit(1)

# Continue with data processing
retail_data = retail_data.dropna(subset=['CustomerID'])
retail_data['CustomerID'] = retail_data['CustomerID'].astype(int)

# Set up database connection using direct SQLite connection
print(f"Creating SQLite database at: {db_file}")
try:
    # Create orders table using groupby
    print("Creating orders table...")
    orders = retail_data.groupby(['InvoiceNo', 'CustomerID', 'InvoiceDate']).agg({
        'Quantity': 'sum', 
        'UnitPrice': lambda x: (x * retail_data.loc[x.index, 'Quantity']).sum()
    }).reset_index()
    
    orders.columns = ['order_id', 'customer_id', 'order_date', 'total_items', 'total_amount']
    orders['status'] = 'Delivered'  # Add a default status
    
    # Remove any potential duplicates
    orders = orders.drop_duplicates(subset=['order_id'])
    
    # Create order_items table
    print("Creating order_items table...")
    order_items = retail_data[['InvoiceNo', 'StockCode', 'Quantity', 'UnitPrice']].copy()
    order_items.columns = ['order_id', 'product_id', 'quantity', 'price']
    
    # Create a direct connection to SQLite
    conn = sqlite3.connect(db_file)
    
    # Save to database
    print("Saving orders to database...")
    orders.to_sql('orders', conn, if_exists='replace', index=False)
    print(f"Successfully saved {len(orders)} orders to database")
    
    print("Saving order items to database...")
    order_items.to_sql('order_items', conn, if_exists='replace', index=False)
    print(f"Successfully saved {len(order_items)} order items to database")
    
    # Verify the data was loaded
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders")
    order_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM order_items")
    item_count = cursor.fetchone()[0]
    
    print(f"Verification: {order_count} orders and {item_count} order items in the database")
    
    # Close the connection
    conn.close()
    
    print("Database setup completed successfully!")
    
except Exception as e:
    print(f"Error setting up database: {e}")
    import traceback
    traceback.print_exc()