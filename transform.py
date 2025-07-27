#!/usr/bin/env python3
"""
Weather Data Transformation Module

Processes weather data using PySpark for cleaning, aggregation, and analysis.
"""

import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, mean, sum as spark_sum, 
    to_date, year, month, dayofmonth, round as spark_round, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType
)
from config import config


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
        Exception: If SparkSession creation fails
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
        
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        raise


def load_weather_data(spark: SparkSession, filepath: str) -> DataFrame:
    """
    Load weather data from CSV file into Spark DataFrame.
    
    Args:
        spark: SparkSession instance
        filepath: Path to the weather CSV file
        
    Returns:
        Spark DataFrame with weather data
        
    Raises:
        Exception: If data loading fails
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
        return df
        
    except Exception as e:
        logger.error(f"Failed to load weather data from {filepath}: {e}")
        raise


def clean_weather_data(df: DataFrame) -> DataFrame:
    """
    Clean weather data by handling missing values and data types.
    
    Args:
        df: Raw weather DataFrame
        
    Returns:
        Cleaned DataFrame with proper data types
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Starting data cleaning process")
    
    # Convert date column to proper date type
    df_clean = df.withColumn("date", to_date(col("DATE"), "yyyy-MM-dd"))
    
    # Convert numeric columns with proper null handling
    for col_name in config.NUMERIC_COLUMNS:
        df_clean = df_clean.withColumn(
            col_name.lower(),
            when(col(col_name) == "", lit(None).cast("double"))
            .otherwise(col(col_name).cast("double"))
        )
    
    # Add derived columns
    df_clean = df_clean.withColumn("year", year(col("date"))) \
                      .withColumn("month", month(col("date"))) \
                      .withColumn("day", dayofmonth(col("date")))
    
    # Convert temperatures from tenths of degrees to degrees
    df_clean = df_clean.withColumn(
        "tmax_celsius", 
        spark_round(col("tmax") / config.TEMPERATURE_DIVISOR, config.TEMPERATURE_PRECISION)
    ).withColumn(
        "tmin_celsius", 
        spark_round(col("tmin") / config.TEMPERATURE_DIVISOR, config.TEMPERATURE_PRECISION)
    )
    
    # Calculate daily average temperature
    df_clean = df_clean.withColumn(
        "tavg_celsius",
        spark_round((col("tmax_celsius") + col("tmin_celsius")) / 2.0, config.TEMPERATURE_PRECISION)
    )
    
    # Select final columns
    df_result = df_clean.select(
        col("date"),
        col("STATION").alias("station"),
        col("year"),
        col("month"), 
        col("day"),
        col("tmax_celsius"),
        col("tmin_celsius"),
        col("tavg_celsius"),
        col("prcp"),
        col("snow"),
        col("snwd"),
        col("awnd")
    )
    
    logger.info("Data cleaning completed")
    return df_result


def compute_daily_aggregations(df: DataFrame) -> DataFrame:
    """
    Compute daily weather aggregations and statistics.
    
    Args:
        df: Cleaned weather DataFrame
        
    Returns:
        DataFrame with daily aggregations
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Computing daily weather aggregations")
    
    # Group by date and compute aggregations
    daily_agg = df.groupBy("date", "station", "year", "month", "day") \
                  .agg(
                      mean("tmax_celsius").alias("avg_tmax"),
                      mean("tmin_celsius").alias("avg_tmin"),
                      mean("tavg_celsius").alias("avg_temperature"),
                      spark_sum("prcp").alias("total_precipitation"),
                      spark_sum("snow").alias("total_snow"),
                      mean("awnd").alias("avg_wind_speed")
                  )
    
    # Round aggregated values
    daily_agg = daily_agg.withColumn("avg_tmax", spark_round(col("avg_tmax"), config.TEMPERATURE_PRECISION)) \
                        .withColumn("avg_tmin", spark_round(col("avg_tmin"), config.TEMPERATURE_PRECISION)) \
                        .withColumn("avg_temperature", spark_round(col("avg_temperature"), config.TEMPERATURE_PRECISION)) \
                        .withColumn("avg_wind_speed", spark_round(col("avg_wind_speed"), config.WIND_PRECISION))
    
    # Order by date
    daily_agg = daily_agg.orderBy("date")
    
    logger.info("Daily aggregations computed successfully")
    return daily_agg


def save_processed_data(df: DataFrame, output_path: Optional[str] = None, 
                       partition_cols: Optional[list] = None) -> None:
    """
    Save processed data as partitioned Parquet files.
    
    Args:
        df: DataFrame to save
        output_path: Path to save the Parquet files
        partition_cols: Columns to partition by
        
    Raises:
        Exception: If data saving fails
    """
    logger = logging.getLogger(__name__)
    
    if output_path is None:
        output_path = config.get_output_path()
    
    if partition_cols is None:
        partition_cols = config.PARTITION_COLUMNS
    
    logger.info(f"Saving processed data to {output_path}")
    logger.info(f"Partitioning by: {partition_cols}")
    
    try:
        df.write \
          .mode("overwrite") \
          .partitionBy(*partition_cols) \
          .parquet(output_path)
        
        logger.info(f"Successfully saved processed data to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save processed data to {output_path}: {e}")
        raise


def create_temp_view(spark: SparkSession, df: DataFrame, 
                    view_name: Optional[str] = None) -> None:
    """
    Create a temporary SQL view for the processed data.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to create view from
        view_name: Name for the temporary view
        
    Raises:
        Exception: If view creation fails
    """
    logger = logging.getLogger(__name__)
    
    if view_name is None:
        view_name = config.DEFAULT_VIEW_NAME
    
    try:
        df.createOrReplaceTempView(view_name)
        logger.info(f"Created temporary view: {view_name}")
        
        # Show sample queries
        logger.info("Sample query results:")
        sample_query = f"""
        SELECT date, avg_temperature, total_precipitation
        FROM {view_name}
        ORDER BY date DESC
        LIMIT 10
        """
        
        spark.sql(sample_query).show()
        
    except Exception as e:
        logger.error(f"Failed to create temporary view {view_name}: {e}")
        raise


def generate_profile_report(df: DataFrame, output_path: Optional[str] = None) -> None:
    """
    Generate a simple data profile report and save as markdown.
    
    Args:
        df: Spark DataFrame to profile
        output_path: Path to save the markdown report
        
    Raises:
        Exception: If report generation fails
    """
    import pandas as pd
    logger = logging.getLogger(__name__)
    
    if output_path is None:
        output_path = str(config.PROFILE_REPORT_PATH)
    
    logger.info(f"Generating data profile report at {output_path}")
    
    try:
        profile = []
        row_count = df.count()
        
        profile.append("# Data Profile Report\n\n")
        profile.append(f"**Row count:** {row_count}\n\n")
        profile.append("| Column | Nulls | Min | Max |\n|---|---|---|---|\n")
        
        for col_name, dtype in df.dtypes:
            nulls = df.filter(df[col_name].isNull()).count()
            min_val = df.agg({col_name: 'min'}).collect()[0][0]
            max_val = df.agg({col_name: 'max'}).collect()[0][0]
            profile.append(f"| {col_name} | {nulls} | {min_val} | {max_val} |\n")
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, "w") as f:
            f.writelines(profile)
        
        logger.info(f"Profile report saved to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to generate profile report: {e}")
        raise


def transform_weather_data(input_path: Optional[str] = None, 
                          output_path: Optional[str] = None) -> None:
    """
    Main transformation function that orchestrates the entire process.
    
    Args:
        input_path: Path to input CSV file
        output_path: Path to save processed Parquet files
        
    Raises:
        Exception: If transformation process fails
    """
    logger = logging.getLogger(__name__)
    
    if input_path is None:
        input_path = config.get_input_path()
    
    if output_path is None:
        output_path = config.get_output_path()
    
    spark = None
    try:
        logger.info("Starting weather data transformation")
        
        # Ensure directories exist
        config.ensure_directories()
        
        # Create Spark session
        spark = create_spark_session()
        
        # Load data
        df_raw = load_weather_data(spark, input_path)
        
        # Clean data
        df_clean = clean_weather_data(df_raw)
        
        # Compute aggregations
        df_agg = compute_daily_aggregations(df_clean)
        
        # Save processed data
        save_processed_data(df_agg, output_path)
        
        # Create SQL view
        create_temp_view(spark, df_agg)
        
        # Generate profile report
        generate_profile_report(df_agg)
        
        logger.info("Weather data transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Weather data transformation failed: {e}")
        raise
    finally:
        if spark:
            logger.info("Stopping SparkSession")
            spark.stop()


def main() -> None:
    """Main function to run the transformation process."""
    setup_logging()
    transform_weather_data()


if __name__ == "__main__":
    main() 