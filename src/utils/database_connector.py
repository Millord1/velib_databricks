from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
import psycopg2
import os

class DatabaseConnector(ABC):
    
    def __init__(self, host: str, user: str, db_name: str, port: int):
        self.host = host
        self.user = user
        self.db_name = db_name
        self.port = port
        self.station_table = "station"
        self.releve_table = "releve"

    @abstractmethod
    def save_data(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        ...
        
    @abstractmethod
    def create_tables(self) -> None:
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
                id SERIAL PRIMARY KEY,
                station_id BIGINT REFERENCES {self.station_table}(station_id),
                num_bikes_available INTEGER,
                num_docks_available INTEGER,
                releve_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            # 4. Releve Index 
            f"""
            CREATE INDEX IF NOT EXISTS idx_{self.releve_table}_station_time 
            ON {self.releve_table} (station_id, releve_time DESC);
            """
        ]
        
        
        
class JDBCSparkConnector(DatabaseConnector):
    
    def __init__(self, host: str, user: str, db_name: str, port: int):
        super().__init__(host, user, db_name, port)
        
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.db_name}"
        
        spark = SparkSession.builder.getOrCreate()
        self.jvm = spark._jvm
        
        self.db_properties = {
            "user": self.user,
            "password": self._get_password(),
            "driver": "org.postgresql.Driver"
        }
        
        self.conn = None
        self.stmt = None

    def __enter__(self):
        self._initialize_database()
        self._connect(self.jdbc_url)
        self._create_tables()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.stmt:
            self.stmt.close()
        if self.conn:
            self.conn.close()
            
    def _get_password(self) -> str:
        try:
            import dbutils
            return dbutils.secrets.get(scope="portfolio_secrets", key="postgres_password")
        except (NameError, ModuleNotFoundError):
            return os.getenv("POSTGRES_PASSWORD")

    def _connect(self, url: str):
        props = self.jvm.java.util.Properties()
        props.setProperty("user", self.user)
        props.setProperty("password", self._get_password())
        
        self.conn = self.jvm.java.sql.DriverManager.getConnection(url, props)
        self.stmt = self.conn.createStatement()

    def _initialize_database(self) -> None:
        url_default = f"jdbc:postgresql://{self.host}:{self.port}/postgres"
        
        self._connect(url_default)
        rs = self.stmt.executeQuery(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.db_name}'")
        
        if not rs.next():
            self.stmt.executeUpdate(f"CREATE DATABASE {self.db_name}")
            
        rs.close()
        self.stmt.close()

    def _create_tables(self) -> None:
        for query in self.get_ddl_queries():
            self.stmt.executeUpdate(query)

    def save_data(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        
        if table_name is not self.station_table or not self.releve_table:
            raise NameError("Table name not available")
        
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .options(**self.db_properties) \
            .mode(mode) \
            .save()
            
            

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
        
        if table_name is not self.station_table or not self.releve_table:
            raise NameError("Table name not available")

        pandas_df = df.toPandas()
        
        # TODO push data

        self.conn.commit()