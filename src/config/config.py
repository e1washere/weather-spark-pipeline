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
    LOGS_DIR = Path("logs")
    
    # File patterns
    CSV_PATTERN = "weather_data_*.csv"
    PARQUET_PATTERN = "weather_parquet"
    
    # Spark configuration
    SPARK_APP_NAME = "WeatherETL"
    SPARK_CONFIGS = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m"
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
    
    # Data validation rules
    VALIDATION_RULES = {
        "temperature_range": {"min": -50, "max": 60},
        "precipitation_range": {"min": 0, "max": 1000},
        "snow_range": {"min": 0, "max": 500},
        "wind_range": {"min": 0, "max": 100},
        "required_columns": ["DATE", "STATION", "TMAX", "TMIN"],
        "date_format": "%Y-%m-%d"
    }
    
    # Performance thresholds
    PERFORMANCE_THRESHOLDS = {
        "max_processing_time_seconds": 300,
        "min_data_quality_score": 0.85,
        "max_memory_usage_gb": 4,
        "expected_record_count_min": 100
    }
    
    # SQL view settings
    DEFAULT_VIEW_NAME = "weather_data"
    
    # Logging
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_LEVEL = "INFO"
    LOG_FILE = "weather_etl.log"
    
    # Monitoring
    METRICS_ENABLED = True
    METRICS_INTERVAL_SECONDS = 30
    
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
        cls.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        cls.PROFILE_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)


# Global config instance
config = WeatherConfig() 