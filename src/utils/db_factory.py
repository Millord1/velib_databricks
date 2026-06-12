# src/utils/db_factory.py
import os
from src.utils.database_connector import DatabaseConnector, DatabricksDeltaConnector, LocalPostgresConnector


def get_db_connector(is_databricks: bool) -> DatabaseConnector:
    
    if is_databricks:
        print("Databricks env detected")

        return DatabricksDeltaConnector(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER", "postgres"),
            db_name=os.getenv("DB_NAME", "velib"),
            port=int(os.getenv("DB_PORT", 5432))
        )
        
    else:
        print("Local env detected")
        
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
            
        return LocalPostgresConnector(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "postgres"),
            db_name=os.getenv("DB_NAME", "velib"),
            port=int(os.getenv("DB_PORT", 5432))
        )