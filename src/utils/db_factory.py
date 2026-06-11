# src/utils/db_factory.py
import os
from src.utils.database_connector import DatabaseConnector, JDBCSparkConnector, LocalPostgresConnector
from dotenv import load_dotenv

def get_db_connector() -> DatabaseConnector:
    
    # Environment detection
    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
    
    if is_databricks:
        print("Databricks env detected")

        return JDBCSparkConnector(
            host=os.getenv("DB_HOST", "ton-rds-ou-supabase.com"),
            user=os.getenv("DB_USER", "db_admin_prod"),
            db_name=os.getenv("DB_NAME", "velib_prod"),
            port=int(os.getenv("DB_PORT", 5432))
        )
        
    else:
        print("Local env detected")
        load_dotenv()
            
        return LocalPostgresConnector(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "postgres"),
            db_name=os.getenv("DB_NAME", "velib_dev"),
            port=int(os.getenv("DB_PORT", 5432))
        )