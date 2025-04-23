import json
import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for e-commerce data',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Path to SQLite database
DB_FILE = '/opt/airflow/data/ecommerce.db'

# Custom data quality operator class
class DataQualityOperator:
    def __init__(self, task_id, df=None, checks=None):
        self.task_id = task_id
        self.df = df
        self.checks = checks or []
        
    def execute(self, context):
        if self.df is None:
            print("DataFrame is None, skipping checks")
            return
            
        if not isinstance(self.df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
            
        if not self.checks:
            print("No checks provided, skipping")
            return
            
        failed_checks = []
        
        for i, check in enumerate(self.checks):
            print(f"Running check {i+1}: {check.get('description', 'Unnamed Check')}")
            
            # Get check details
            check_type = check.get('type', 'value')
            column = check.get('column')
            
            if check_type == 'not_empty':
                result = len(self.df) > 0
                expected = True
                print(f"Check result: DataFrame {'is not' if result else 'is'} empty")
                
            elif check_type == 'no_nulls':
                result = self.df[column].isnull().sum() == 0
                expected = True
                null_count = self.df[column].isnull().sum()
                print(f"Check result: Column '{column}' has {null_count} null values")
                
            elif check_type == 'unique':
                result = self.df[column].is_unique
                expected = True
                duplicate_count = len(self.df) - len(self.df[column].unique())
                print(f"Check result: Column '{column}' has {duplicate_count} duplicate values")
                
            elif check_type == 'min_value':
                min_val = check.get('value')
                result = self.df[column].min() >= min_val
                expected = True
                actual_min = self.df[column].min()
                print(f"Check result: Minimum value in '{column}' is {actual_min}, expected >= {min_val}")
                
            elif check_type == 'max_value':
                max_val = check.get('value')
                result = self.df[column].max() <= max_val
                expected = True
                actual_max = self.df[column].max()
                print(f"Check result: Maximum value in '{column}' is {actual_max}, expected <= {max_val}")
                
            else:
                print(f"Warning: Unknown check type: {check_type}")
                continue
                
            if result != expected:
                failed_checks.append({
                    'check': check,
                    'actual': result,
                    'expected': expected
                })
                
        if failed_checks:
            for fail in failed_checks:
                print(f"Failed check: {fail['check'].get('description', 'Unnamed Check')}")
                print(f"Expected: {fail['expected']}, Got: {fail['actual']}")
            raise ValueError(f"Data quality check failed: {len(failed_checks)} checks failed")
            
        print("All data quality checks passed!")

# Helper functions
def clean_customer_data(df):
    """Clean and standardize customer data from Olist dataset."""
    # Make a copy to avoid SettingWithCopyWarning
    df_clean = df.copy()
    
    # Rename columns to match our schema
    column_mapping = {
        'customer_id': 'customer_id',
        'customer_unique_id': 'unique_id',
        'customer_zip_code_prefix': 'zipcode',
        'customer_city': 'city',
        'customer_state': 'state'
    }
    
    # Apply renaming if columns exist
    existing_columns = set(df_clean.columns)
    rename_dict = {k: v for k, v in column_mapping.items() if k in existing_columns}
    
    if rename_dict:
        df_clean.rename(columns=rename_dict, inplace=True)
    
    # Add missing columns
    if 'first_name' not in df_clean.columns:
        df_clean['first_name'] = 'Unknown'
    
    if 'last_name' not in df_clean.columns:
        df_clean['last_name'] = 'Unknown'
    
    if 'email' not in df_clean.columns:
        df_clean['email'] = 'unknown@example.com'
    
    if 'phone' not in df_clean.columns:
        df_clean['phone'] = 'Unknown'
    
    if 'address' not in df_clean.columns:
        df_clean['address'] = 'Unknown'
    
    if 'registration_date' not in df_clean.columns:
        df_clean['registration_date'] = pd.Timestamp.now().date()
    
    # Clean string columns
    for col in ['city', 'state']:
        if col in df_clean.columns and df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].str.strip().str.title()
    
    return df_clean

def clean_product_data(products_data):
    """Clean and standardize product data from Amazon dataset."""
    # Convert to DataFrame if it's a list
    if isinstance(products_data, list):
        df = pd.DataFrame(products_data)
    else:
        df = products_data.copy()
    
    # Ensure required columns exist
    required_columns = ['product_id', 'name', 'description', 'price']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in product data")
    
    # Clean string columns
    for col in ['name', 'description']:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].fillna('').str.strip()
    
    # Ensure numeric columns are correct type
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    
    if 'list_price' in df.columns:
        df['list_price'] = pd.to_numeric(df['list_price'], errors='coerce').fillna(0)
    
    if 'units_sold' in df.columns:
        df['units_sold'] = pd.to_numeric(df['units_sold'], errors='coerce').fillna(0).astype(int)
    
    if 'rating' in df.columns:
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)
    
    # Add a category column if not present
    if 'category' not in df.columns and 'color' in df.columns:
        # Use color as a proxy for category if needed
        df['category'] = 'Clothing & Accessories'
    elif 'category' not in df.columns:
        df['category'] = 'Uncategorized'
    
    # Add a stock quantity if not present
    if 'stock_quantity' not in df.columns and 'units_sold' in df.columns:
        import random

        # Generate a reasonable stock based on units sold
        df['stock_quantity'] = df['units_sold'].apply(lambda x: max(0, int(x * 0.5) + random.randint(5, 50)))
    elif 'stock_quantity' not in df.columns:
        df['stock_quantity'] = 100  # Default value
    
    # Add an added_date if not present
    if 'added_date' not in df.columns:
        df['added_date'] = pd.Timestamp.now().date()
    
    return df

def parse_web_logs(logs_data):
    """Parse web logs from CSV format."""
    # If logs_data is a string (file content), try to parse it
    if isinstance(logs_data, str):
        try:
            # Try to parse as CSV
            return pd.read_csv(pd.StringIO(logs_data))
        except:
            # If not CSV, raise error
            raise ValueError("Web logs must be in CSV format")
    elif isinstance(logs_data, pd.DataFrame):
        # If already a DataFrame, return a copy
        return logs_data.copy()
    else:
        raise ValueError("Unexpected data type for web logs")

def enrich_order_data(orders_df, items_df):
    """Combine order and order items data from UK Online Retail dataset."""
    # Make copies to avoid SettingWithCopyWarning
    orders = orders_df.copy()
    items = items_df.copy()
    
    # Ensure column names are correct
    expected_order_cols = ['order_id', 'customer_id', 'order_date', 'total_amount', 'status']
    expected_item_cols = ['order_id', 'product_id', 'quantity', 'price']
    
    # Check if essential columns exist
    for col in expected_order_cols:
        if col not in orders.columns:
            raise ValueError(f"Required column '{col}' not found in orders data")
    
    for col in expected_item_cols:
        if col not in items.columns:
            raise ValueError(f"Required column '{col}' not found in order items data")
    
    # Calculate total items per order if not already present
    if 'total_items' not in orders.columns:
        # Calculate total items for each order
        items_count = items.groupby('order_id')['quantity'].sum().reset_index()
        items_count.rename(columns={'quantity': 'total_items'}, inplace=True)
        
        # Merge with orders
        orders = orders.merge(items_count, on='order_id', how='left')
        
        # Fill missing values
        orders['total_items'] = orders['total_items'].fillna(0).astype(int)
    
    # Ensure date is in correct format
    if 'order_date' in orders.columns:
        orders['order_date'] = pd.to_datetime(orders['order_date'])
    
    return orders

# Task functions
def extract_customer_data(**kwargs):
    """Extract customer data from Olist dataset CSV file."""
    try:
        # Get the path to the customers file
        customers_file = '/opt/airflow/data/customers/customers.csv'
        
        # Read the CSV file
        customers_df = pd.read_csv(customers_file)
        
        # Store DataFrame in XCom for the next task
        kwargs['ti'].xcom_push(key='customers_df', value=customers_df.to_json(orient='records'))
        
        return f"Successfully extracted {len(customers_df)} customer records"
    except Exception as e:
        raise Exception(f"Error extracting customer data: {e}")

def extract_product_data(**kwargs):
    """Extract product data from JSON file (converted from Amazon dataset)."""
    try:
        # Get the path to the products file
        products_file = '/opt/airflow/data/products/products.json'
        
        # Read the JSON file
        with open(products_file, 'r') as f:
            products_data = json.load(f)
        
        # Store data in XCom for the next task
        kwargs['ti'].xcom_push(key='products_data', value=products_data)
        
        return f"Successfully extracted {len(products_data)} product records"
    except Exception as e:
        raise Exception(f"Error extracting product data: {e}")

def extract_web_logs(**kwargs):
    """Extract web traffic logs from CSV file."""
    try:
        # Get the path to the web logs file
        logs_file = '/opt/airflow/data/logs/web_logs.csv'
        
        # Read the CSV file
        logs_df = pd.read_csv(logs_file)
        
        # Store data in XCom for the next task
        kwargs['ti'].xcom_push(key='logs_df', value=logs_df.to_json(orient='records'))
        
        return f"Successfully extracted {len(logs_df)} web log records"
    except Exception as e:
        raise Exception(f"Error extracting web logs: {e}")

def extract_order_data(**kwargs):
    """Extract order data from SQLite database."""
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_FILE)
        
        # Query orders table
        orders_df = pd.read_sql("SELECT * FROM orders", conn)
        
        # Query order_items table
        items_df = pd.read_sql("SELECT * FROM order_items", conn)
        
        # Close the connection
        conn.close()
        
        # Store DataFrames in XCom for the next task
        kwargs['ti'].xcom_push(key='orders_df', value=orders_df.to_json(orient='records'))
        kwargs['ti'].xcom_push(key='items_df', value=items_df.to_json(orient='records'))
        
        return f"Successfully extracted {len(orders_df)} order records and {len(items_df)} order item records"
    except Exception as e:
        raise Exception(f"Error extracting order data: {e}")

def transform_customer_data(**kwargs):
    """Transform customer data."""
    try:
        # Get customers data from previous task
        ti = kwargs['ti']
        customers_json = ti.xcom_pull(task_ids='extract_customer_data', key='customers_df')
        customers_df = pd.read_json(customers_json, orient='records')
        
        # Apply transformations
        transformed_df = clean_customer_data(customers_df)
        
        # Quality checks
        quality_checks = [
            {'type': 'not_empty', 'description': 'Customer data not empty'},
            {'type': 'no_nulls', 'column': 'customer_id', 'description': 'No null customer IDs'},
            {'type': 'unique', 'column': 'customer_id', 'description': 'Customer IDs are unique'},
        ]
        
        # Create and execute the data quality check operator
        quality_check = DataQualityOperator(
            task_id='customer_quality_check',
            df=transformed_df,
            checks=quality_checks
        )
        
        # Execute the quality check
        quality_check.execute(context=kwargs)
        
        # Store transformed data
        ti.xcom_push(key='transformed_customers', value=transformed_df.to_json(orient='records'))
        
        return f"Successfully transformed {len(transformed_df)} customer records"
    except Exception as e:
        raise Exception(f"Error transforming customer data: {e}")

def transform_product_data(**kwargs):
    """Transform product data."""
    try:
        # Get product data from previous task
        ti = kwargs['ti']
        products_data = ti.xcom_pull(task_ids='extract_product_data', key='products_data')
        
        # Apply transformations
        transformed_df = clean_product_data(products_data)
        
        # Quality checks
        quality_checks = [
            {'type': 'not_empty', 'description': 'Product data not empty'},
            {'type': 'no_nulls', 'column': 'product_id', 'description': 'No null product IDs'},
            {'type': 'unique', 'column': 'product_id', 'description': 'Product IDs are unique'},
            {'type': 'min_value', 'column': 'price', 'value': 0, 'description': 'Prices are non-negative'}
        ]
        
        # Create and execute the data quality check operator
        quality_check = DataQualityOperator(
            task_id='product_quality_check',
            df=transformed_df,
            checks=quality_checks
        )
        
        # Execute the quality check
        quality_check.execute(context=kwargs)
        
        # Store transformed data
        ti.xcom_push(key='transformed_products', value=transformed_df.to_json(orient='records'))
        
        return f"Successfully transformed {len(transformed_df)} product records"
    except Exception as e:
        raise Exception(f"Error transforming product data: {e}")

def transform_web_logs(**kwargs):
    """Transform web logs data."""
    try:
        # Get web logs data from previous task
        ti = kwargs['ti']
        logs_json = ti.xcom_pull(task_ids='extract_web_logs', key='logs_df')
        logs_df = pd.read_json(logs_json, orient='records')
        
        # Apply transformations
        logs_df['timestamp'] = pd.to_datetime(logs_df['timestamp'])
        
        # Quality checks
        quality_checks = [
            {'type': 'not_empty', 'description': 'Web logs data not empty'},
            {'type': 'no_nulls', 'column': 'timestamp', 'description': 'No null timestamps'},
            {'type': 'no_nulls', 'column': 'ip_address', 'description': 'No null IP addresses'}
        ]
        
        # Create and execute the data quality check operator
        quality_check = DataQualityOperator(
            task_id='logs_quality_check',
            df=logs_df,
            checks=quality_checks
        )
        
        # Execute the quality check
        quality_check.execute(context=kwargs)
        
        # Store transformed data
        ti.xcom_push(key='transformed_logs', value=logs_df.to_json(orient='records'))
        
        return f"Successfully transformed {len(logs_df)} log records"
    except Exception as e:
        raise Exception(f"Error transforming web logs: {e}")

def transform_order_data(**kwargs):
    """Transform order data."""
    try:
        # Get orders data from previous task
        ti = kwargs['ti']
        orders_json = ti.xcom_pull(task_ids='extract_order_data', key='orders_df')
        items_json = ti.xcom_pull(task_ids='extract_order_data', key='items_df')
        
        # Convert to DataFrames
        orders_df = pd.read_json(orders_json, orient='records')
        items_df = pd.read_json(items_json, orient='records')
        
        # Apply transformations
        transformed_df = enrich_order_data(orders_df, items_df)
        
        # Quality checks
        quality_checks = [
            {'type': 'not_empty', 'description': 'Order data not empty'},
            {'type': 'no_nulls', 'column': 'order_id', 'description': 'No null order IDs'},
            {'type': 'unique', 'column': 'order_id', 'description': 'Order IDs are unique'},
            {'type': 'min_value', 'column': 'total_amount', 'value': 0, 'description': 'Total amounts are non-negative'}
        ]
        
        # Create and execute the data quality check operator
        quality_check = DataQualityOperator(
            task_id='order_quality_check',
            df=transformed_df,
            checks=quality_checks
        )
        
        # Execute the quality check
        quality_check.execute(context=kwargs)
        
        # Store transformed data
        ti.xcom_push(key='transformed_orders', value=transformed_df.to_json(orient='records'))
        
        return f"Successfully transformed {len(transformed_df)} order records"
    except Exception as e:
        raise Exception(f"Error transforming order data: {e}")

def load_to_warehouse(**kwargs):
    """Load all transformed data to the data warehouse."""
    try:
        # Get transformed data from previous tasks
        ti = kwargs['ti']
        customers_json = ti.xcom_pull(task_ids='transform_customer_data', key='transformed_customers')
        products_json = ti.xcom_pull(task_ids='transform_product_data', key='transformed_products')
        logs_json = ti.xcom_pull(task_ids='transform_web_logs', key='transformed_logs')
        orders_json = ti.xcom_pull(task_ids='transform_order_data', key='transformed_orders')
        
        # Convert to DataFrames
        customers_df = pd.read_json(customers_json, orient='records')
        products_df = pd.read_json(products_json, orient='records')
        logs_df = pd.read_json(logs_json, orient='records')
        orders_df = pd.read_json(orders_json, orient='records')
        
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Create the schema tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zipcode TEXT,
            registration_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id INTEGER PRIMARY KEY,
            name TEXT,
            description TEXT,
            category TEXT,
            price REAL,
            stock_quantity INTEGER,
            added_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL NOT NULL,
            total_items INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fact_web_traffic (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            ip_address TEXT,
            user_agent TEXT,
            page TEXT,
            status_code INTEGER,
            bytes INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        
        # Function to insert or update records
        def upsert_data(df, table_name, key_column):
            # Clear existing data
            cursor.execute(f"DELETE FROM {table_name}")
            
            # Use pandas to_sql for efficient insertion
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
            return len(df)
        
        # Load data to the data warehouse tables
        customers_count = upsert_data(customers_df, 'dim_customers', 'customer_id')
        products_count = upsert_data(products_df, 'dim_products', 'product_id')
        orders_count = upsert_data(orders_df, 'fact_orders', 'order_id')
        
        # For web logs, we don't have a natural key, so just append
        logs_df.to_sql('fact_web_traffic', conn, if_exists='append', index=False)
        
        # Commit and close
        conn.commit()
        conn.close()
        
        return f"Successfully loaded data to warehouse: {customers_count} customers, {products_count} products, {orders_count} orders, {len(logs_df)} web logs"
    except Exception as e:
        raise Exception(f"Error loading data to warehouse: {e}")

# Define the tasks
extract_customer_task = PythonOperator(
    task_id='extract_customer_data',
    python_callable=extract_customer_data,
    provide_context=True,
    dag=dag,
)

extract_product_task = PythonOperator(
    task_id='extract_product_data',
    python_callable=extract_product_data,
    provide_context=True,
    dag=dag,
)

extract_web_logs_task = PythonOperator(
    task_id='extract_web_logs',
    python_callable=extract_web_logs,
    provide_context=True,
    dag=dag,
)

extract_order_task = PythonOperator(
    task_id='extract_order_data',
    python_callable=extract_order_data,
    provide_context=True,
    dag=dag,
)

transform_customer_task = PythonOperator(
    task_id='transform_customer_data',
    python_callable=transform_customer_data,
    provide_context=True,
    dag=dag,
)

transform_product_task = PythonOperator(
    task_id='transform_product_data',
    python_callable=transform_product_data,
    provide_context=True,
    dag=dag,
)

transform_web_logs_task = PythonOperator(
    task_id='transform_web_logs',
    python_callable=transform_web_logs,
    provide_context=True,
    dag=dag,
)

transform_order_task = PythonOperator(
    task_id='transform_order_data',
    python_callable=transform_order_data,
    provide_context=True,
    dag=dag,
)

load_warehouse_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
extract_customer_task >> transform_customer_task
extract_product_task >> transform_product_task
extract_web_logs_task >> transform_web_logs_task
extract_order_task >> transform_order_task

# All transformations must complete before loading
[transform_customer_task, transform_product_task, 
 transform_web_logs_task, transform_order_task] >> load_warehouse_task
