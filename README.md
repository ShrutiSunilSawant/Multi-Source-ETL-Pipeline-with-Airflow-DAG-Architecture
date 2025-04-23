# Multi-Source-ETL-Pipeline-with-Airflow-DAG-Architecture

## Project Overview
This project implements a comprehensive ETL (Extract, Transform, Load) pipeline that processes data from multiple sources, transforms it according to business rules, and loads it into a dimensional data warehouse for analysis.

## Data Sources
- **Customer Data**: Brazilian E-commerce dataset (CSV)
- **Product Data**: Amazon product dataset (JSON)
- **Order Data**: UK Online Retail dataset (SQLite)
- **Web Traffic**: Synthetic web server logs (CSV)

## Features
- Extract data from multiple sources with different formats
- Transform data with data quality checks and error handling
- Load data into a dimensional data warehouse model
- Orchestrate the pipeline with Airflow DAG architecture
- Handle errors and data quality issues gracefully

## Technical Architecture
- **Languages**: Python
- **Data Handling**: Pandas, SQLite
- **File Formats**: CSV, JSON, SQL
- **Workflow**: DAG-based task orchestration
- **Error Handling**: Comprehensive exception management

## Project Structure
ecommerce_etl/
├── dags/                      # DAG definitions
│   └── ecommerce_pipeline.py  # Airflow DAG definition
├── data/                      # Data sources
│   ├── customers/             # Customer data files
│   ├── products/              # Product data files
│   ├── orders/                # Order data files
│   ├── logs/                  # Web log files
│   └── ecommerce.db           # SQLite database
├── scripts/                   # Utility scripts
│   ├── prepare_product_data.py  # Prepare product JSON
│   ├── generate_web_logs.py     # Generate web logs
│   └── setup_database.py        # Set up order database
├── airflow_etl_simulation.py  # ETL pipeline with Airflow simulation
├── requirements.txt           # Project dependencies
└── README.md                  # Project documentation

## Running the Pipeline

1. Install required dependencies:
   pip install -r requirements.txt

2. Set up the data directories and prepare data:
   python scripts/prepare_product_data.py
   python scripts/generate_web_logs.py
   python scripts/setup_database.py

3. Run the ETL pipeline:
   python airflow_etl_simulation.py

4. Verify the results in the SQLite database (`data/ecommerce.db`):
- `dim_customers`: Customer dimension
- `dim_products`: Product dimension
- `fact_orders`: Order transactions
- `fact_web_traffic`: Web traffic data

## Data Transformations
- Customer data: Cleaning, standardization, NULL handling
- Product data: Type conversions, category standardization
- Order data: Negative amount correction, aggregation
- Web logs: Timestamp parsing, structure normalization

## Results
The pipeline processes over 130,000 records from four distinct data sources:
- 99,441 customer records
- 1,341 product records
- 18,536 order records
- 10,000 web traffic records

## Technical Challenges Addressed
- Schema evolution and mismatches
- Data type conversions
- Primary key handling (string vs. integer)
- Missing value imputation
- Error recovery
- Task dependency management
