# src/pipelines/historical_pipeline.py
import os
import shutil
import kagglehub
import pandas as pd
from pyspark.sql import SparkSession
from src.utils.database_connector import DatabaseConnector

def download_and_move_dataset() -> str:

    target_dir = os.path.join(os.getcwd(), "data")
    target_file = os.path.join(target_dir, "velib_historique.parquet")
    
    if os.path.exists(target_file):
        print(f"File {target_file} already existing, no download to do")
        return target_file

    cache_path = kagglehub.dataset_download("adrienmorel97/velib-data")
    
    files = [f for f in os.listdir(cache_path) if f.endswith('.parquet')]
    if not files:
        raise FileNotFoundError("No file .parquet found in the downloaded dataset")
    
    source_file = os.path.join(cache_path, files[0])
    os.makedirs(target_dir, exist_ok=True)
    shutil.move(source_file, target_file)
    
    return target_file


def clean_stations(df: pd.DataFrame) -> pd.DataFrame:
    
    return (df[['station_id', 'name', 'lat', 'lon', 'capacity']]
            .assign(
                station_id=lambda x: x['station_id'].astype(int),
                capacity=lambda x: x['capacity'].astype(int),
                station_code=lambda x: x['station_id'].astype(str)
            )
            .drop_duplicates(subset=['station_id']))


def clean_meteo(df: pd.DataFrame) -> pd.DataFrame:
    return (df[['lat', 'lon', 'ts_utc', 'temp_C', 'precip_mm', 'wind_mps']]
            .assign(
                # keep only hour, no need to keep ms
                meteo_time=lambda x: pd.to_datetime(x['ts_utc'], unit='ms').dt.floor('h'),
                # creating area (trunc to 2 decimals), avoid massive redundancy
                lat_zone=lambda x: x['lat'].round(2),
                lon_zone=lambda x: x['lon'].round(2)
            )
            .rename(columns={
                'temp_C': 'temperature',
                'precip_mm': 'precipitation',
                'wind_mps': 'wind_speed'
            })
            [['lat_zone', 'lon_zone', 'meteo_time', 'temperature', 'precipitation', 'wind_speed']]
            .drop_duplicates(subset=['lat_zone', 'lon_zone', 'meteo_time']))


def clean_releves(df: pd.DataFrame) -> pd.DataFrame:
    
    return (df[['station_id', 'bikes', 'capacity', 'ts_utc']]
            .assign(
                station_id=lambda x: x['station_id'].astype(int),
                releve_time=lambda x: pd.to_datetime(x['ts_utc'], unit='ms'),
                num_bikes_available=lambda x: x['bikes'].astype(int),
                num_docks_available=lambda x: x['capacity'].astype(int) - x['bikes'].astype(int)
            )
            [['station_id', 'num_bikes_available', 'num_docks_available', 'releve_time']]
            .drop_duplicates(subset=['station_id', 'releve_time']))


def run_historical_pipeline(db: DatabaseConnector) -> None:
    
    file_path = download_and_move_dataset()
    df_raw = pd.read_parquet(file_path)
    
    df_stations_pd = df_raw.pipe(clean_stations)
    df_meteo_pd = df_raw.pipe(clean_meteo)
    df_releves_pd = df_raw.pipe(clean_releves)

    spark = SparkSession.builder.getOrCreate()

    print("Preparing to push historical data to database...")

    with db as db_conn:
        print("Pushing stations data...")
        spark_stations = spark.createDataFrame(df_stations_pd)
        db_conn.save_data(spark_stations, table_name=db_conn.station_table, mode="overwrite")
        
        print("Pushing weather data...")
        spark_weather = spark.createDataFrame(df_meteo_pd)
        db_conn.save_data(spark_weather, table_name=db_conn.weather_table, mode="append")
        
        print("Pushing releve data...")
        spark_releves = spark.createDataFrame(df_releves_pd)
        db_conn.save_data(spark_releves, table_name=db_conn.releve_table, mode="append")
        