# ecommerce-etl-pipeline
An end-to-end Data Engineering pipeline designed to automate the Extraction, Transformation, and Loading (ETL) of global superstore sales data. This project demonstrates a production-like approach to handling messy real-world data, including encoding issues, international date formats, and schema evolution.


🚀 Project Overview
- The goal of this pipeline is to take raw CSV data (Global Superstore Sales), perform complex cleaning and feature engineering, and load it into a structured MySQL database for analytical use.
Key Features:
- Robust Extraction: Handles various file encodings (ISO-8859-1) and potential file-read errors.
- Defensive Transformation: Cleans column headers (lowercasing, stripping spaces) and handles missing columns (like postal_code) dynamically.
- International Date Support: Correctly parses DD/MM/YYYY formats to prevent data corruption.
- Incremental Loading: Checks the database for the last loaded record to avoid duplicate entries and save processing time.
- Production Logging: Maintains a detailed audit trail in logs/etl_pipeline.log.
- Data Validation: Post-load checks to ensure row counts and data integrity.


🛠️ Tech Stack

Language: Python 3.10+

Data Manipulation: Pandas, NumPy

Database Engine: MySQL Workbench

Database Connector: SQLAlchemy, PyMySQL

Automation: Logging, Try-Except error handling


📁 Project Structure

ecommerce_etl/
│
├── data/                  # Source CSV files (Kaggle Superstore)
│   └── superstore.csv
├── logs/                  # Automated pipeline execution logs
│   └── etl_pipeline.log
├── etl_main.py            # Main Python ETL logic
└── README.md              # Project documentation


🧠 Challenges & Solutions

During development, several real-world data issues were identified and resolved:
- Encoding Error: Resolved 'charmap' codec can't decode byte 0x9d by implementing ISO-8859-1 encoding detection.
- Date Parsing: Fixed a crash where 13/01/2011 was interpreted as an invalid month by utilizing dayfirst=True in Pandas.
- Schema Mismatch: Handled a market column mismatch by implementing a defensive transformation layer that adapts to the CSV's available columns.
- Missing Data: Injected dummy values for missing postal_code fields to maintain database schema integrity.
