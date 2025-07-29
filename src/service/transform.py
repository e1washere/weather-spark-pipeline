#!/usr/bin/env python3
"""
Weather Data Transformation Module

Handles data cleaning, transformation, and aggregation using PySpark.
"""

import logging
import os
from pathlib import Path
from typing import Optional, List
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, mean, sum as spark_sum,
    to_date, year, month, dayofmonth, round as spark_round, lit, count
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from py4j.protocol import Py4JJavaError

from ..config import config
from .validation import validate_weather_data
from .monitoring import performance_monitor, log_dataframe_stats, create_performance_report, save_performance_report


def setup_logging() -> None:
    """Configure logging for the transformation process."""
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=config.LOG_FORMAT
    )


def create_spark_session(app_name: Optional[str] = None) -> SparkSession:
    """
    Create and configure SparkSession for weather data processing.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        Configured SparkSession
        
    Raises:
        Py4JJavaError: If SparkSession creation fails
        ValueError: If app_name is invalid
    """
    logger = logging.getLogger(__name__)
    
    if app_name is None:
        app_name = config.SPARK_APP_NAME
    
    try:
        builder = SparkSession.builder.appName(app_name)
        
        # Apply all Spark configurations
        for key, value in config.SPARK_CONFIGS.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        logger.info(f"Created SparkSession: {app_name}")
        return spark
        
    except Py4JJavaError as e:
        logger.error(f"Failed to create SparkSession '{app_name}': {e}")
        raise Py4JJavaError(f"Spark initialization failed for '{app_name}': {e}")
    except ValueError as e:
        logger.error(f"Invalid Spark configuration: {e}")
        raise ValueError(f"Invalid Spark configuration for '{app_name}': {e}")


def load_weather_data(spark: SparkSession, filepath: str) -> DataFrame:
    """
    Load weather data from CSV file into Spark DataFrame.
    
    Args:
        spark: SparkSession instance
        filepath: Path to the weather CSV file
        
    Returns:
        Spark DataFrame with weather data
        
    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        Py4JJavaError: If Spark encounters an error or CSV format is invalid
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Loading weather data from {filepath}")
        
        df = spark.read \
            .option("header", "true") \
            .schema(config.WEATHER_SCHEMA) \
            .csv(filepath)
        
        record_count = df.count()
        logger.info(f"Loaded {record_count} records from {filepath}")
        
        # Log DataFrame statistics
        log_dataframe_stats(df, "LOAD")
        
        return df
        
    except FileNotFoundError:
        logger.error(f"Input file not found: {filepath}")
        raise FileNotFoundError(f"Cannot load weather data: file {filepath} does not exist")
    except Py4JJavaError as e:
        logger.error(f"Spark error while loading {filepath}: {e}")
        raise Py4JJavaError(f"Spark failed to process {filepath}: {e}")


def clean_weather_data(df: DataFrame) -> DataFrame:
    """
    Clean and validate weather data.
    
    Args:
        df: Raw weather DataFrame
        
    Returns:
        Cleaned DataFrame
        
    Raises:
        ValueError: If data validation fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting data cleaning process")
    
    # Log initial statistics
    log_dataframe_stats(df, "BEFORE_CLEANING")
    
    # Perform data validation
    validation_results = validate_weather_data(df)
    
    if not validation_results["overall_valid"]:
        logger.warning("Data validation failed, but continuing with cleaning")
        logger.warning(f"Validation issues: {validation_results['issues']}")
    
    # Clean numeric columns
    df_clean = df
    for col_name in config.NUMERIC_COLUMNS:
        if col_name in df.columns:
            df_clean = df_clean.withColumn(
                col_name.lower(),
                when(col(col_name) == "", lit(None).cast("double"))
                .otherwise(col(col_name).cast("double"))
            )
    
    # Remove rows with missing required data
    required_cols = [col.lower() for col in config.VALIDATION_RULES["required_columns"] if col.lower() in df_clean.columns]
    if required_cols:
        df_clean = df_clean.dropna(subset=required_cols)
    
    # Add date parsing
    if "date" in df_clean.columns:
        df_clean = df_clean.withColumn(
            "date",
            to_date(col("date"), config.VALIDATION_RULES["date_format"])
        )
    
    # Add partitioning columns
    if "date" in df_clean.columns:
        df_clean = df_clean.withColumn("year", year(col("date")))
        df_clean = df_clean.withColumn("month", month(col("date")))
    
    # Log final statistics
    log_dataframe_stats(df_clean, "AFTER_CLEANING")
    
    logger.info("Data cleaning completed")
    return df_clean


def compute_daily_aggregations(df: DataFrame) -> DataFrame:
    """
    Compute daily weather aggregations.
    
    Args:
        df: Cleaned weather DataFrame
        
    Returns:
        DataFrame with daily aggregations
    """
    logger = logging.getLogger(__name__)
    logger.info("Computing daily aggregations")
    
    # Group by date and compute aggregations
    daily_agg = df.groupBy("date", "station") \
        .agg(
            mean("tmax").alias("avg_max_temp"),
            mean("tmin").alias("avg_min_temp"),
            spark_sum("prcp").alias("total_precipitation"),
            spark_sum("snow").alias("total_snow"),
            mean("awnd").alias("avg_wind_speed")
        )
    
    # Round numeric values
    daily_agg = daily_agg.withColumn(
        "avg_max_temp",
        spark_round(col("avg_max_temp"), config.TEMPERATURE_PRECISION)
    ).withColumn(
        "avg_min_temp",
        spark_round(col("avg_min_temp"), config.TEMPERATURE_PRECISION)
    ).withColumn(
        "avg_wind_speed",
        spark_round(col("avg_wind_speed"), config.WIND_PRECISION)
    )
    
    # Add partitioning columns
    daily_agg = daily_agg.withColumn("year", year(col("date")))
    daily_agg = daily_agg.withColumn("month", month(col("date")))
    
    logger.info("Daily aggregations computed")
    return daily_agg


def save_processed_data(df: DataFrame, output_path: Optional[str] = None, 
                       partition_cols: Optional[list] = None) -> None:
    """
    Save processed DataFrame to Parquet format.
    
    Args:
        df: DataFrame to save
        output_path: Output directory path
        partition_cols: Columns to partition by
        
    Raises:
        Py4JJavaError: If Spark save operation fails
        ValueError: If partition columns are invalid
        OSError: If output directory cannot be created
    """
    logger = logging.getLogger(__name__)
    
    if output_path is None:
        output_path = config.get_output_path()
    
    if partition_cols is None:
        partition_cols = config.PARTITION_COLUMNS
    
    try:
        # Ensure output directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Save DataFrame
        writer = df.write.mode("overwrite").format("parquet")
        
        if partition_cols and all(col in df.columns for col in partition_cols):
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(output_path)
        
        logger.info(f"Data saved to {output_path}")
        
    except Py4JJavaError as e:
        logger.error(f"Spark failed to save data to {output_path}: {e}")
        raise Py4JJavaError(f"Data save operation failed for {output_path}: {e}")
    except ValueError as e:
        logger.error(f"Invalid partition configuration: {e}")
        raise ValueError(f"Invalid partition columns {partition_cols}: {e}")
    except OSError as e:
        logger.error(f"Cannot create output directory {output_path}: {e}")
        raise OSError(f"Failed to create output directory {output_path}: {e}")


def create_temp_view(spark: SparkSession, df: DataFrame, 
                    view_name: Optional[str] = None) -> None:
    """
    Create temporary SQL view for data analysis.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to create view from
        view_name: Name for the temporary view
        
    Raises:
        Py4JJavaError: If view creation fails
        ValueError: If view name is invalid
    """
    logger = logging.getLogger(__name__)
    
    if view_name is None:
        view_name = config.DEFAULT_VIEW_NAME
    
    try:
        df.createOrReplaceTempView(view_name)
        logger.info(f"Created temporary view: {view_name}")
        
        # Verify view creation
        result = spark.sql(f"SELECT COUNT(*) as count FROM {view_name}").collect()
        logger.info(f"View {view_name} contains {result[0]['count']} records")
        
    except Py4JJavaError as e:
        logger.error(f"Spark failed to create view '{view_name}': {e}")
        raise Py4JJavaError(f"SQL view creation failed for '{view_name}': {e}")
    except ValueError as e:
        logger.error(f"Invalid view name '{view_name}': {e}")
        raise ValueError(f"Invalid SQL view name '{view_name}': {e}")


def generate_profile_report(df: DataFrame, output_path: Optional[str] = None) -> None:
    """
    Generate data profiling report.
    
    Args:
        df: DataFrame to profile
        output_path: Path for the profile report
        
    Raises:
        ValueError: If DataFrame is empty
        Py4JJavaError: If profiling fails
        OSError: If report cannot be written
    """
    logger = logging.getLogger(__name__)
    
    if output_path is None:
        output_path = str(config.PROFILE_REPORT_PATH)
    
    try:
        row_count = df.count()
        if row_count == 0:
            raise ValueError("Cannot generate profile for empty DataFrame")
        
        # Generate basic statistics
        profile_data = []
        for col_name in df.columns:
            col_stats = df.select(
                count(col(col_name)).alias("count"),
                count(when(col(col_name).isNull(), True)).alias("nulls")
            ).collect()[0]
            
            profile_data.append({
                "column": col_name,
                "count": col_stats["count"],
                "nulls": col_stats["nulls"],
                "null_percentage": (col_stats["nulls"] / row_count * 100) if row_count > 0 else 0
            })
        
        # Write profile report
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("# Weather Data Profile Report\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Total Records: {row_count}\n\n")
            f.write("## Column Statistics\n\n")
            f.write("| Column | Count | Nulls | Null % |\n")
            f.write("|--------|-------|-------|--------|\n")
            
            for col_data in profile_data:
                f.write(f"| {col_data['column']} | {col_data['count']} | {col_data['nulls']} | {col_data['null_percentage']:.1f}% |\n")
        
        logger.info(f"Profile report generated: {output_path}")
        
    except ValueError as e:
        logger.error(f"Invalid data for profiling: {e}")
        raise ValueError(f"Data profiling error: {e}")
    except Py4JJavaError as e:
        logger.error(f"Spark failed to generate profile: {e}")
        raise Py4JJavaError(f"Data profiling failed: {e}")
    except OSError as e:
        logger.error(f"Cannot write profile report to {output_path}: {e}")
        raise OSError(f"Failed to write profile report to {output_path}: {e}")


def transform_weather_data(input_path: Optional[str] = None, 
                          output_path: Optional[str] = None) -> None:
    """
    Complete weather data transformation pipeline.
    
    Args:
        input_path: Path to input CSV file(s)
        output_path: Path for output Parquet files
        
    Raises:
        FileNotFoundError: If input file not found
        Py4JJavaError: If Spark processing fails
        OSError: If file system operations fail
        ValueError: If data validation fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Start performance monitoring
        performance_monitor.start_monitoring()
        
        # Setup logging
        setup_logging()
        
        # Ensure directories exist
        config.ensure_directories()
        
        # Create Spark session
        spark = create_spark_session()
        
        # Load data
        if input_path is None:
            input_path = config.get_input_path()
        
        df = load_weather_data(spark, input_path)
        
        # Clean data
        df_clean = clean_weather_data(df)
        
        # Compute aggregations
        df_agg = compute_daily_aggregations(df_clean)
        
        # Save processed data
        save_processed_data(df_agg, output_path)
        
        # Create SQL view
        create_temp_view(spark, df_agg)
        
        # Generate profile report
        generate_profile_report(df_clean)
        
        # End performance monitoring
        performance_monitor.end_monitoring()
        
        # Create and save performance report
        validation_results = validate_weather_data(df_clean)
        performance_report = create_performance_report(performance_monitor, validation_results)
        save_performance_report(performance_report)
        
        logger.info("Weather data transformation completed successfully")
        
    except (FileNotFoundError, Py4JJavaError, OSError, ValueError) as e:
        logger.error(f"Weather data transformation failed: {e}")
        raise


def main() -> None:
    """Main function for standalone execution."""
    transform_weather_data()


if __name__ == "__main__":
    main() 