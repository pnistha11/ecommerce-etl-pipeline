import pandas as pd
import logging
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# --- CONFIGURATION FOR MYSQL ---
DB_TYPE = 'mysql+pymysql'  # Changed from postgresql
DB_USER = 'root'           # Default MySQL user is usually 'root'
DB_PASS = ''  # The password you set in MySQL Workbench
DB_HOST = 'localhost'
DB_PORT = '3306'           # MySQL default port is 3306
DB_NAME = 'ecommerce_db'
TABLE_NAME = 'stg_superstore_sales'
CSV_PATH = 'data/superstore.csv'

# Setup Logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SuperstoreETL:
    def __init__(self):
        # Updated connection string for MySQL
        self.connection_string = f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        # We add 'pool_pre_ping' to ensure the connection stays alive
        self.engine = create_engine(self.connection_string, pool_pre_ping=True)

    def extract(self):
        """Step 1: Extract data from CSV with robust encoding handling."""
        try:
            logging.info("Starting Extraction...")
            
            # Try loading with 'latin1' or 'ISO-8859-1' 
            # These encodings are standard for Superstore/Excel CSVs
            # We also add engine='python' to handle complex formatting
            df = pd.read_csv(
                CSV_PATH, 
                encoding='ISO-8859-1', 
                engine='python',
                on_bad_lines='skip' # Optional: skip lines that are completely broken
            )
            
            logging.info(f"Extraction successful. Rows loaded: {len(df)}")
            return df
        except Exception as e:
            logging.error(f"Extraction failed: {e}")
            # If ISO-8859-1 also fails, try 'cp1252' (Windows-1252)
            try:
                logging.info("Attempting fallback encoding cp1252...")
                df = pd.read_csv(CSV_PATH, encoding='cp1252')
                return df
            except Exception as final_e:
                logging.error(f"All extraction attempts failed: {final_e}")
                raise

    def transform(self, df):
        """Step 2: Clean and Transform with Defensive Column Handling."""
        try:
            logging.info("Starting Transformation...")

            # 1. Clean column names: strip spaces, lowercase, replace separators
            df.columns = [col.strip().lower().replace(' ', '_').replace('.', '_').replace('-', '_') for col in df.columns]
            
            # --- DEBUG: Print columns so you can see them in your terminal ---
            print("Available columns in your CSV after cleaning:")
            print(df.columns.tolist())
            # -----------------------------------------------------------------

            # 2. Convert Date columns
            if 'order_date' in df.columns:
                df['order_date'] = pd.to_datetime(df['order_date'], dayfirst=True, errors='coerce')
            if 'ship_date' in df.columns:
                df['ship_date'] = pd.to_datetime(df['ship_date'], dayfirst=True, errors='coerce')

            # 3. Defensive Postal Code handling (The Fix for your Error)
            if 'postal_code' in df.columns:
                df['postal_code'] = df['postal_code'].fillna(0).astype(str)
            elif 'zip_code' in df.columns: # Sometimes it's called Zip Code
                df['postal_code'] = df['zip_code'].fillna(0).astype(str)
                logging.info("Renamed 'zip_code' to 'postal_code'")
            else:
                logging.warning("Postal code column not found. Creating a dummy column.")
                df['postal_code'] = "00000"

            # 4. Remove Duplicates
            df = df.drop_duplicates()

            # 5. Ensure Sales and Quantity are numeric for calculation
            # Use 'sales' if it exists, otherwise check for 'revenue' or similar
            sales_col = 'sales' if 'sales' in df.columns else None
            qty_col = 'quantity' if 'quantity' in df.columns else None

            if sales_col and qty_col:
                df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
                df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
                
                # New calculated columns
                df['revenue'] = df[sales_col]
                df['unit_price'] = df[sales_col] / df[qty_col].replace(0, 1)

            # 6. Final check: drop rows where critical data is missing
            df = df.dropna(subset=['order_id'])

            logging.info(f"Transformation successful. Columns: {df.columns.tolist()}")
            return df
        except Exception as e:
            logging.error(f"Transformation failed: {e}")
            raise

    def get_last_load_date(self):
        """Check the database for the most recent order_date (Incremental Loading)."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT MAX(order_date) FROM {TABLE_NAME}"))
                max_date = result.scalar()
                return max_date
        except Exception:
            # If table doesn't exist, return a very old date
            return None

    def load(self, df):
        """Step 3: Load data into SQL."""
        try:
            logging.info("Starting Load...")
            
            # CHANGE 'append' to 'replace' for this run to fix the schema mismatch
            df.to_sql(TABLE_NAME, self.engine, if_exists='replace', index=False)
            
            logging.info(f"Load successful. {len(df)} records inserted.")
            
        except Exception as e:
            logging.error(f"Load failed: {e}")
            raise

    def validate_data(self):
        """Step 4: Post-load validation."""
        try:
            with self.engine.connect() as conn:
                # 1. Check for nulls in primary keys
                res = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE order_id IS NULL"))
                null_count = res.scalar()
                
                # 2. Check total row count
                res_total = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
                total_count = res_total.scalar()

                logging.info(f"Validation complete. Total rows in DB: {total_count}. Null OrderIDs: {null_count}")
                if null_count > 0:
                    logging.warning("Validation Warning: Null Order IDs found!")
        except Exception as e:
            logging.error(f"Validation step failed: {e}")

    def run_pipeline(self):
        """Orchestrate the ETL steps."""
        print("Starting ETL Process...")
        try:
            data = self.extract()
            cleaned_data = self.transform(data)
            self.load(cleaned_data)
            self.validate_data()
            print("ETL Process completed successfully! Check logs for details.")
        except Exception as e:
            print(f"ETL Process Failed. Error: {e}")

if __name__ == "__main__":
    etl = SuperstoreETL()
    etl.run_pipeline()