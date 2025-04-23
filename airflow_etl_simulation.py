import json
import os
import random
import sqlite3
import time
from datetime import datetime

import pandas as pd


# Simulate Airflow's DAG structure
class Task:
    def __init__(self, task_id, python_callable, depends_on=None):
        self.task_id = task_id
        self.python_callable = python_callable
        self.depends_on = depends_on or []
        self.xcom_push_data = {}
        
    def execute(self, context):
        print(f"\n[{datetime.now()}] Executing task: {self.task_id}")
        start_time = time.time()
        
        # Create a task instance object for xcom
        ti = TaskInstance(self)
        task_context = {**context, 'ti': ti}
        
        try:
            result = self.python_callable(**task_context)
            duration = time.time() - start_time
            print(f"Task {self.task_id} completed successfully in {duration:.2f} seconds")
            print(f"Result: {result}")
            return True
        except Exception as e:
            print(f"Task {self.task_id} failed: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    def set_downstream(self, tasks):
        if not isinstance(tasks, list):
            tasks = [tasks]
        for task in tasks:
            task.depends_on.append(self)
        return self
        
    def __rshift__(self, other):
        return self.set_downstream(other)

class TaskInstance:
    def __init__(self, task):
        self.task = task
        
    def xcom_push(self, key, value):
        self.task.xcom_push_data[key] = value
        
    def xcom_pull(self, task_ids, key):
        for task in dag.tasks:
            if task.task_id == task_ids:
                return task.xcom_push_data.get(key)
        return None

class DAG:
    def __init__(self, dag_id, default_args, description, schedule_interval):
        self.dag_id = dag_id
        self.default_args = default_args
        self.description = description
        self.schedule_interval = schedule_interval
        self.tasks = []
        
    def add_task(self, task):
        self.tasks.append(task)
        
    def run(self):
        # Build dependency graph
        executed_tasks = set()
        all_tasks = set(self.tasks)
        
        while executed_tasks != all_tasks:
            # Find tasks with all dependencies satisfied
            runnable_tasks = [
                task for task in all_tasks - executed_tasks
                if all(dep in executed_tasks for dep in task.depends_on)
            ]
            
            if not runnable_tasks:
                remaining = all_tasks - executed_tasks
                print(f"\nError: Could not resolve dependencies for remaining tasks: {[t.task_id for t in remaining]}")
                return False
                
            for task in runnable_tasks:
                success = task.execute({'dag': self})
                executed_tasks.add(task)
                if not success:
                    print(f"\nTask {task.task_id} failed. Stopping DAG execution.")
                    return False
            
        print("\nDAG execution completed successfully!")
        return True

# Get the project root directory
project_dir = os.path.dirname(os.path.abspath(__file__))

# Define file paths
customers_file = os.path.join(project_dir, 'data', 'customers', 'customers.csv')
products_file = os.path.join(project_dir, 'data', 'products', 'products.json')
web_logs_file = os.path.join(project_dir, 'data', 'logs', 'web_logs.csv')
db_file = os.path.join(project_dir, 'data', 'ecommerce.db')

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
            print(f"Required column '{col}' not found. Available columns: {df.columns.tolist()}")
            if col == 'product_id' and 'id' in df.columns:
                df['product_id'] = df['id']
            else:
                df[col] = 'Unknown' if col in ['name', 'description'] else 0
    
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
    if 'category' not in df.columns:
        if 'color' in df.columns:
            # Use color as a proxy for category if needed
            df['category'] = 'Clothing & Accessories'
        else:
            df['category'] = 'Uncategorized'
    
    # Add a stock quantity if not present
    if 'stock_quantity' not in df.columns:
        if 'units_sold' in df.columns:
            # Generate a reasonable stock based on units sold
            df['stock_quantity'] = df['units_sold'].apply(lambda x: max(0, int(x * 0.5) + random.randint(5, 50)))
        else:
            df['stock_quantity'] = 100  # Default value
    
    # Add an added_date if not present
    if 'added_date' not in df.columns:
        df['added_date'] = pd.Timestamp.now().date()
    
    return df

def parse_web_logs(logs_df):
    """Process web logs from CSV format."""
    # Convert timestamp to datetime
    if 'timestamp' in logs_df.columns:
        logs_df['timestamp'] = pd.to_datetime(logs_df['timestamp'])
    
    return logs_df

def enrich_order_data(orders_df, items_df):
    """Combine order and order items data."""
    # Make copies to avoid SettingWithCopyWarning
    orders = orders_df.copy()
    items = items_df.copy()
    
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
    """Extract customer data from CSV file."""
    try:
        print(f"Reading customer data from: {customers_file}")
        customers_df = pd.read_csv(customers_file)
        kwargs['ti'].xcom_push('customers_df', customers_df)
        return f"Successfully extracted {len(customers_df)} customer records"
    except Exception as e:
        raise Exception(f"Error extracting customer data: {e}")

def extract_product_data(**kwargs):
    """Extract product data from JSON file."""
    try:
        print(f"Reading product data from: {products_file}")
        with open(products_file, 'r') as f:
            products_data = json.load(f)
        kwargs['ti'].xcom_push('products_data', products_data)
        return f"Successfully extracted {len(products_data)} product records"
    except Exception as e:
        raise Exception(f"Error extracting product data: {e}")

def extract_web_logs(**kwargs):
    """Extract web traffic logs from CSV file."""
    try:
        print(f"Reading web logs from: {web_logs_file}")
        logs_df = pd.read_csv(web_logs_file)
        kwargs['ti'].xcom_push('logs_df', logs_df)
        return f"Successfully extracted {len(logs_df)} web log records"
    except Exception as e:
        raise Exception(f"Error extracting web logs: {e}")

def extract_order_data(**kwargs):
    """Extract order data from SQLite database."""
    try:
        print(f"Reading order data from: {db_file}")
        conn = sqlite3.connect(db_file)
        orders_df = pd.read_sql("SELECT * FROM orders", conn)
        items_df = pd.read_sql("SELECT * FROM order_items", conn)
        conn.close()
        
        kwargs['ti'].xcom_push('orders_df', orders_df)
        kwargs['ti'].xcom_push('items_df', items_df)
        
        return f"Successfully extracted {len(orders_df)} order records and {len(items_df)} order item records"
    except Exception as e:
        raise Exception(f"Error extracting order data: {e}")

def transform_customer_data(**kwargs):
    """Transform customer data."""
    try:
        # Get customers data from previous task
        ti = kwargs['ti']
        customers_df = ti.xcom_pull('extract_customer_data', 'customers_df')
        
        # Apply transformations
        transformed_df = clean_customer_data(customers_df)
        
        # Perform data quality checks
        if len(transformed_df) == 0:
            raise ValueError("Customer data is empty after transformation")
            
        if transformed_df['customer_id'].isnull().any():
            raise ValueError("Found null customer IDs after transformation")
            
        # Store transformed data
        ti.xcom_push('transformed_customers', transformed_df)
        
        return f"Successfully transformed {len(transformed_df)} customer records"
    except Exception as e:
        raise Exception(f"Error transforming customer data: {e}")

def transform_product_data(**kwargs):
    """Transform product data."""
    try:
        # Get product data from previous task
        ti = kwargs['ti']
        products_data = ti.xcom_pull('extract_product_data', 'products_data')
        
        # Apply transformations
        transformed_df = clean_product_data(products_data)
        
        # Perform data quality checks
        if len(transformed_df) == 0:
            raise ValueError("Product data is empty after transformation")
            
        if transformed_df['product_id'].isnull().any():
            raise ValueError("Found null product IDs after transformation")
            
        if (transformed_df['price'] < 0).any():
            raise ValueError("Found negative prices in product data")
        
        # Store transformed data
        ti.xcom_push('transformed_products', transformed_df)
        
        return f"Successfully transformed {len(transformed_df)} product records"
    except Exception as e:
        raise Exception(f"Error transforming product data: {e}")

def transform_web_logs(**kwargs):
    """Transform web logs data."""
    try:
        # Get web logs data from previous task
        ti = kwargs['ti']
        logs_df = ti.xcom_pull('extract_web_logs', 'logs_df')
        
        # Apply transformations
        transformed_df = parse_web_logs(logs_df)
        
        # Perform data quality checks
        if len(transformed_df) == 0:
            raise ValueError("Web logs data is empty after transformation")
        
        # Store transformed data
        ti.xcom_push('transformed_logs', transformed_df)
        
        return f"Successfully transformed {len(transformed_df)} log records"
    except Exception as e:
        raise Exception(f"Error transforming web logs: {e}")

def transform_order_data(**kwargs):
    """Transform order data."""
    try:
        # Get orders data from previous task
        ti = kwargs['ti']
        orders_df = ti.xcom_pull('extract_order_data', 'orders_df')
        items_df = ti.xcom_pull('extract_order_data', 'items_df')
        
        # Apply transformations
        transformed_df = enrich_order_data(orders_df, items_df)
        
        # Perform data quality checks
        if len(transformed_df) == 0:
            raise ValueError("Order data is empty after transformation")
            
        if transformed_df['order_id'].isnull().any():
            raise ValueError("Found null order IDs after transformation")
            
        # Handle negative total amounts by taking the absolute value
        if (transformed_df['total_amount'] < 0).any():
            print(f"WARNING: Found {(transformed_df['total_amount'] < 0).sum()} negative total amounts in order data. Converting to absolute values.")
            transformed_df['total_amount'] = transformed_df['total_amount'].abs()
        
        # Store transformed data
        ti.xcom_push('transformed_orders', transformed_df)
        
        return f"Successfully transformed {len(transformed_df)} order records"
    except Exception as e:
        raise Exception(f"Error transforming order data: {e}")

def load_to_warehouse(**kwargs):
    """Load all transformed data to the data warehouse."""
    try:
        # Get transformed data from previous tasks
        ti = kwargs['ti']
        customers_df = ti.xcom_pull('transform_customer_data', 'transformed_customers')
        products_df = ti.xcom_pull('transform_product_data', 'transformed_products')
        logs_df = ti.xcom_pull('transform_web_logs', 'transformed_logs')
        orders_df = ti.xcom_pull('transform_order_data', 'transformed_orders')
        
        # Connect to the SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        print("Creating data warehouse schema with TEXT primary keys...")
        
        # Drop existing tables to ensure clean schemas
        cursor.execute("DROP TABLE IF EXISTS dim_customers")
        cursor.execute("DROP TABLE IF EXISTS dim_products")
        cursor.execute("DROP TABLE IF EXISTS fact_orders")
        cursor.execute("DROP TABLE IF EXISTS fact_web_traffic")
        
        # Create the schema tables with TEXT primary keys for customers and products
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id TEXT PRIMARY KEY,
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
            product_id TEXT PRIMARY KEY,
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
        
        print("Loading data to warehouse tables...")
        
        # Function for manually loading customers
        def load_customers(df):
            # Fix customer_id as string
            data = []
            for _, row in df.iterrows():
                try:
                    customer_id = str(row['customer_id'])
                    first_name = str(row['first_name'] if not pd.isna(row['first_name']) else '')
                    last_name = str(row['last_name'] if not pd.isna(row['last_name']) else '')
                    email = str(row['email'] if not pd.isna(row['email']) else '')
                    phone = str(row['phone'] if not pd.isna(row['phone']) else '')
                    address = str(row['address'] if not pd.isna(row['address']) else '')
                    city = str(row['city'] if not pd.isna(row['city']) else '')
                    state = str(row['state'] if not pd.isna(row['state']) else '')
                    zipcode = str(row['zipcode'] if not pd.isna(row['zipcode']) else '')
                    
                    # Handle registration date
                    if 'registration_date' in row and not pd.isna(row['registration_date']):
                        if isinstance(row['registration_date'], pd.Timestamp):
                            registration_date = row['registration_date'].strftime('%Y-%m-%d')
                        else:
                            registration_date = str(row['registration_date'])
                    else:
                        registration_date = '2023-01-01'
                    
                    data.append((
                        customer_id, first_name, last_name, email, phone, 
                        address, city, state, zipcode, registration_date
                    ))
                except Exception as e:
                    print(f"Error processing customer row: {e}")
                    continue
            
            cursor.executemany(
                """
                INSERT INTO dim_customers 
                (customer_id, first_name, last_name, email, phone, address, city, state, zipcode, registration_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                data
            )
            conn.commit()
            return len(data)
        
        # Function for manually loading products
        def load_products(df):
            if df['product_id'].duplicated().any():
                print(f"WARNING: Found {df['product_id'].duplicated().sum()} duplicate product IDs. Keeping only the first occurrence.")
        # Keep only the first occurrence of each product_id
                df = df.drop_duplicates(subset=['product_id'], keep='first')
    
            data = []
            for _, row in df.iterrows():
                try:
                    product_id = str(row['product_id'])
                    name = str(row['name'] if not pd.isna(row['name']) else '')
                    description = str(row['description'] if not pd.isna(row['description']) else '')
                    category = str(row['category'] if not pd.isna(row['category']) else '')
                    price = float(row['price'] if not pd.isna(row['price']) else 0.0)
                    stock_quantity = int(row['stock_quantity'] if not pd.isna(row['stock_quantity']) else 0)
            
            # Handle added date
                    if 'added_date' in row and not pd.isna(row['added_date']):
                        if isinstance(row['added_date'], pd.Timestamp):
                            added_date = row['added_date'].strftime('%Y-%m-%d')
                        else:
                            added_date = str(row['added_date'])
                    else:
                        added_date = '2023-01-01'
                    data.append((product_id, name, description, category, price, stock_quantity, added_date))
                except Exception as e:
                    print(f"Error processing product row: {e}")
                    continue
    
            cursor.executemany(
                """
                INSERT INTO dim_products 
                (product_id, name, description, category, price, stock_quantity, added_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, 
                data
            )
            conn.commit()
            return len(data)
        
        # Function for loading order data
        def load_orders(df):
            data = []
            for _, row in df.iterrows():
                try:
                    order_id = int(row['order_id'])
                    customer_id = int(row['customer_id'])
                    
                    # Handle order date
                    if isinstance(row['order_date'], pd.Timestamp):
                        order_date = row['order_date'].strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        order_date = str(row['order_date'])
                    
                    total_amount = float(row['total_amount'])
                    total_items = int(row['total_items'])
                    status = str(row['status'])
                    
                    data.append((order_id, customer_id, order_date, total_amount, total_items, status))
                except Exception as e:
                    print(f"Error processing order row: {e}")
                    continue
            
            cursor.executemany(
                """
                INSERT INTO fact_orders 
                (order_id, customer_id, order_date, total_amount, total_items, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """, 
                data
            )
            conn.commit()
            return len(data)
        
        # Function for loading web log data
        def load_logs(df):
            # Convert timestamp to string if it's datetime
            if 'timestamp' in df.columns and pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Filter columns we need
            needed_cols = ['timestamp', 'ip_address', 'user_agent', 'page', 'status_code', 'bytes']
            available_cols = [col for col in needed_cols if col in df.columns]
            filtered_df = df[available_cols].copy()
            
            # Prepare data tuples
            data = []
            for _, row in filtered_df.iterrows():
                values = []
                for col in available_cols:
                    if col in row:
                        # Convert value to appropriate type
                        if pd.isna(row[col]):
                            if col in ['status_code', 'bytes']:
                                values.append(0)  # Default for numeric
                            else:
                                values.append('')  # Default for text
                        else:
                            values.append(row[col])
                data.append(tuple(values))
            
            # Prepare SQL
            cols_str = ', '.join(available_cols)
            placeholders = ', '.join(['?'] * len(available_cols))
            
            cursor.executemany(
                f"INSERT INTO fact_web_traffic ({cols_str}) VALUES ({placeholders})",
                data
            )
            conn.commit()
            return len(data)
        
        # Load data using specialized functions for each table
        customers_count = load_customers(customers_df)
        products_count = load_products(products_df)
        orders_count = load_orders(orders_df)
        logs_count = load_logs(logs_df)
        
        # Commit and close
        conn.commit()
        conn.close()
        
        return f"Successfully loaded data to warehouse: {customers_count} customers, {products_count} products, {orders_count} orders, {logs_count} web logs"
    except Exception as e:
        print(f"Detailed error in load_to_warehouse: {str(e)}")
        import traceback
        traceback.print_exc()
        raise Exception(f"Error loading data to warehouse: {e}")

# Define our DAG
dag = DAG(
    dag_id='ecommerce_etl_pipeline',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
    },
    description='ETL pipeline for e-commerce data',
    schedule_interval='@daily'
)

# Define tasks
extract_customer_task = Task(
    task_id='extract_customer_data',
    python_callable=extract_customer_data
)
dag.add_task(extract_customer_task)

extract_product_task = Task(
    task_id='extract_product_data',
    python_callable=extract_product_data
)
dag.add_task(extract_product_task)

extract_web_logs_task = Task(
    task_id='extract_web_logs',
    python_callable=extract_web_logs
)
dag.add_task(extract_web_logs_task)

extract_order_task = Task(
    task_id='extract_order_data',
    python_callable=extract_order_data
)
dag.add_task(extract_order_task)

transform_customer_task = Task(
    task_id='transform_customer_data',
    python_callable=transform_customer_data
)
dag.add_task(transform_customer_task)

transform_product_task = Task(
    task_id='transform_product_data',
    python_callable=transform_product_data
)
dag.add_task(transform_product_task)

transform_web_logs_task = Task(
    task_id='transform_web_logs',
    python_callable=transform_web_logs
)
dag.add_task(transform_web_logs_task)

transform_order_task = Task(
    task_id='transform_order_data',
    python_callable=transform_order_data
)
dag.add_task(transform_order_task)

load_warehouse_task = Task(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse
)
dag.add_task(load_warehouse_task)

# Set up task dependencies
extract_customer_task.set_downstream(transform_customer_task)
extract_product_task.set_downstream(transform_product_task)
extract_web_logs_task.set_downstream(transform_web_logs_task)
extract_order_task.set_downstream(transform_order_task)

transform_customer_task.set_downstream(load_warehouse_task)
transform_product_task.set_downstream(load_warehouse_task)
transform_web_logs_task.set_downstream(load_warehouse_task)
transform_order_task.set_downstream(load_warehouse_task)

# Execute the DAG
if __name__ == "__main__":
    print("=" * 80)
    print("RUNNING E-COMMERCE ETL PIPELINE")
    print("=" * 80)
    print(f"\nDAG ID: {dag.dag_id}")
    print(f"Description: {dag.description}")
    print(f"Schedule: {dag.schedule_interval}")
    
    dag.run()
    
    # Print data warehouse stats after successful run
    if db_file and os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        print("\nData Warehouse Statistics:")
        tables = ["dim_customers", "dim_products", "fact_orders", "fact_web_traffic"]
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"- {table}: {count} records")
            except sqlite3.OperationalError:
                print(f"- {table}: Table does not exist")

        conn.close()