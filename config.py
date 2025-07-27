#!/usr/bin/env python3
"""
Configuration module for Weather ETL Pipeline

Contains all configuration settings, file paths, and data schemas.
"""

from pathlib import Path
from typing import List, Dict, Any
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


class WeatherConfig:
    """Configuration class for weather data processing."""
    
    # File paths
    LANDING_DIR = Path("data/landing")
    PROCESSED_DIR = Path("data/processed")
    OUTPUT_DIR = Path("data/output")
    PROFILE_REPORT_PATH = Path("data/profile_report.md")
    
    # File patterns
    CSV_PATTERN = "weather_data_*.csv"
    PARQUET_PATTERN = "weather_parquet"
    
    # Spark configuration
    SPARK_APP_NAME = "WeatherETL"
    SPARK_CONFIGS = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true"
    }
    
    # Data schema
    WEATHER_SCHEMA = StructType([
        StructField("DATE", StringType(), True),
        StructField("STATION", StringType(), True),
        StructField("TMAX", StringType(), True),
        StructField("TMIN", StringType(), True),
        StructField("PRCP", StringType(), True),
        StructField("SNOW", StringType(), True),
        StructField("SNWD", StringType(), True),
        StructField("AWND", StringType(), True)
    ])
    
    # Column mappings
    NUMERIC_COLUMNS = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
    PARTITION_COLUMNS = ["year", "month"]
    
    # Data processing settings
    TEMPERATURE_DIVISOR = 10.0
    TEMPERATURE_PRECISION = 1
    WIND_PRECISION = 1
    
    # SQL view settings
    DEFAULT_VIEW_NAME = "weather_data"
    
    # Logging
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_LEVEL = "INFO"
    
    @classmethod
    def get_input_path(cls) -> str:
        """Get input file path pattern."""
        return str(cls.LANDING_DIR / cls.CSV_PATTERN)
    
    @classmethod
    def get_output_path(cls) -> str:
        """Get output directory path."""
        return str(cls.PROCESSED_DIR / cls.PARQUET_PATTERN)
    
    @classmethod
    def ensure_directories(cls) -> None:
        """Ensure all required directories exist."""
        cls.LANDING_DIR.mkdir(parents=True, exist_ok=True)
        cls.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        cls.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.PROFILE_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)


# Global config instance
config = WeatherConfig() 