from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import psycopg2
from psycopg2.extras import execute_batch
import os

class DatabaseConnector(ABC):
    
    def __init__(self, host: str, user: str, db_name: str, port: int):
        self.host = host
        self.user = user
        self.db_name = db_name
        self.port = port
        self.station_table = "station"
        self.releve_table = "releve"
        self.weather_table = "meteo"
        
        self.tables = {
            self.releve_table,
            self.station_table,
            self.weather_table
        }

    @abstractmethod
    def save_data(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        ...
        
    @abstractmethod
    def _create_tables(self) -> None:
        ...
    
    @abstractmethod
    def _get_password(self) -> str:
        ...
        
    @abstractmethod
    def __enter__(self):
        ...
        
    @abstractmethod
    def __exit__(self, exc_type, exc, tb):
        ...
        
    def get_ddl_queries(self) -> list[str]:
        return [
            # 1. Station Table 
            f"""
            CREATE TABLE IF NOT EXISTS {self.station_table} (
                station_id BIGINT PRIMARY KEY,
                station_code VARCHAR(60),
                name VARCHAR(100),
                lat NUMERIC,
                lon NUMERIC,
                capacity INTEGER
            );
            """,
            # 2. Station Index 
            f"""
            CREATE INDEX IF NOT EXISTS idx_{self.station_table}_code 
            ON {self.station_table} (station_code);
            """,
            
            # 3. Releve Table 
            f"""
            CREATE TABLE IF NOT EXISTS {self.releve_table} (
                station_id BIGINT REFERENCES {self.station_table}(station_id),
                num_bikes_available INTEGER,
                num_docks_available INTEGER,
                releve_time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                PRIMARY KEY (releve_time, station_id)
            );
            """,
            # 4. Releve Index 
            f"""
            CREATE INDEX IF NOT EXISTS idx_{self.releve_table}_station_time 
            ON {self.releve_table} (station_id, releve_time DESC);
            """,
            
            f"""
            CREATE TABLE IF NOT EXISTS {self.weather_table} (
                lat NUMERIC,
                lon NUMERIC,
                meteo_time TIMESTAMP,
                temperature NUMERIC,
                precipitation NUMERIC,
                wind_speed NUMERIC,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (meteo_time, lat, lon)
            );
            """
        ]
        

class DatabricksDeltaConnector(DatabaseConnector):
    
    def __init__(self, host: str, user: str, port: int,  db_name: str = "default", catalog_name: str = "workspace"):
        super().__init__(host, user, db_name, port)
        self.catalog_name = catalog_name
        self.spark = SparkSession.builder.getOrCreate()

    def __enter__(self):
        self._initialize_database()
        self._create_tables()
        return self

    def __exit__(self, exc_type, exc, tb):
        pass
    
    def _get_password(self) -> str:
        try:
            import dbutils
            return dbutils.secrets.get(scope="portfolio_secrets", key="postgres_password")
        except (NameError, ModuleNotFoundError):
            return os.getenv("POSTGRES_PASSWORD")
            
    def _initialize_database(self) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.db_name}")

    def _create_tables(self) -> None:
        pass

    def save_data(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        if table_name not in self.tables:
            raise NameError(f"Table name '{table_name}' not available")
        
        full_table_name = f"{self.catalog_name}.{self.db_name}.{table_name}"
        print(f"Saving into {full_table_name}")
        
        df.write \
            .format("delta") \
            .mode(mode) \
            .saveAsTable(full_table_name)
            
            

class LocalPostgresConnector(DatabaseConnector):
    
    def __init__(self, host: str, user: str, db_name: str, port: int):
        super().__init__(host, user, db_name, port)
        
        self.conn = None
        self.cursor = None
        
    def __enter__(self):
        self.__check_database()
        self._create_tables()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _get_password(self) -> str:
        return os.getenv("POSTGRES_PASSWORD", "")

    def __check_database(self):
        self.__connect_without_db()
        
        self.cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;", (self.db_name,))
        exists = self.cursor.fetchone()

        if not exists:
            self.cursor.execute(f"CREATE DATABASE {self.db_name};")
            
        self.cursor.close()
        self.conn.close()
        
        self.__connect()
        
    def __connect_without_db(self):
        self.conn = psycopg2.connect(
            user=self.user, 
            password=self._get_password(), 
            host=self.host, 
            port=str(self.port)
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
    def __connect(self):
        self.conn = psycopg2.connect(
            dbname=self.db_name, 
            user=self.user, 
            password=self._get_password(), 
            host=self.host, 
            port=str(self.port)
        )
        self.cursor = self.conn.cursor()
        
    def _create_tables(self) -> None:
        for query in self.get_ddl_queries():
            self.cursor.execute(query)
        
        self.conn.commit()

    def save_data(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        if table_name not in self.tables:
            raise NameError(f"Table name '{table_name}' not available. Available: {self.tables}")

        pandas_df = df.toPandas()
        
        columns = ", ".join(pandas_df.columns)
        placeholders = ", ".join(["%s"] * len(pandas_df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
        
        data_to_insert = [tuple(x) for x in pandas_df.to_numpy()]
        
        print(f"Inserting {len(data_to_insert)} lines into {table_name}...", flush=True)
        try:
            execute_batch(self.cursor, query, data_to_insert, page_size=10000)
            self.conn.commit()
            print("Insertion succesfull")
        except Exception as e:
            self.conn.rollback()
            print(f"Err during insertion, rollback : {e}")
            raise e