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
    to_date, year, month, dayofmonth, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType
)


def setup_logging() -> None:
    """Configure logging for the transformation process."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def create_spark_session(app_name: str = "WeatherETL") -> SparkSession:
    """
    Create and configure SparkSession for weather data processing.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        Configured SparkSession
    """
    logger = logging.getLogger(__name__)
    
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
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
    """
    logger = logging.getLogger(__name__)
    
    # Define schema for better performance and type safety
    schema = StructType([
        StructField("DATE", StringType(), True),
        StructField("STATION", StringType(), True),
        StructField("TMAX", StringType(), True),
        StructField("TMIN", StringType(), True),
        StructField("PRCP", StringType(), True),
        StructField("SNOW", StringType(), True),
        StructField("SNWD", StringType(), True),
        StructField("AWND", StringType(), True)
    ])
    
    try:
        logger.info(f"Loading weather data from {filepath}")
        
        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(filepath)
        
        logger.info(f"Loaded {df.count()} records from {filepath}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load weather data: {e}")
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
    
    # Convert numeric columns, treating empty strings as null
    numeric_cols = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
    
    for col_name in numeric_cols:
        df_clean = df_clean.withColumn(
            col_name.lower(),
            when(col(col_name) == "", None)
            .otherwise(col(col_name).cast("double"))
        )
    
    # Add derived columns
    df_clean = df_clean.withColumn("year", year(col("date"))) \
                      .withColumn("month", month(col("date"))) \
                      .withColumn("day", dayofmonth(col("date")))
    
    # Convert temperatures from tenths of degrees to degrees
    df_clean = df_clean.withColumn("tmax_celsius", 
                                  spark_round(col("tmax") / 10.0, 1)) \
                      .withColumn("tmin_celsius", 
                                  spark_round(col("tmin") / 10.0, 1))
    
    # Calculate daily average temperature
    df_clean = df_clean.withColumn(
        "tavg_celsius",
        spark_round((col("tmax_celsius") + col("tmin_celsius")) / 2.0, 1)
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
    daily_agg = daily_agg.withColumn("avg_tmax", spark_round(col("avg_tmax"), 1)) \
                        .withColumn("avg_tmin", spark_round(col("avg_tmin"), 1)) \
                        .withColumn("avg_temperature", spark_round(col("avg_temperature"), 1)) \
                        .withColumn("avg_wind_speed", spark_round(col("avg_wind_speed"), 1))
    
    # Order by date
    daily_agg = daily_agg.orderBy("date")
    
    logger.info("Daily aggregations computed successfully")
    return daily_agg


def save_processed_data(df: DataFrame, output_path: str, partition_cols: Optional[list] = None) -> None:
    """
    Save processed data as partitioned Parquet files.
    
    Args:
        df: DataFrame to save
        output_path: Path to save the Parquet files
        partition_cols: Columns to partition by
    """
    logger = logging.getLogger(__name__)
    
    if partition_cols is None:
        partition_cols = ["year", "month"]
    
    logger.info(f"Saving processed data to {output_path}")
    logger.info(f"Partitioning by: {partition_cols}")
    
    try:
        df.write \
          .mode("overwrite") \
          .partitionBy(*partition_cols) \
          .parquet(output_path)
        
        logger.info(f"Successfully saved processed data to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save processed data: {e}")
        raise


def create_temp_view(spark: SparkSession, df: DataFrame, view_name: str = "weather_data") -> None:
    """
    Create a temporary SQL view for the processed data.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to create view from
        view_name: Name for the temporary view
    """
    logger = logging.getLogger(__name__)
    
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
        logger.error(f"Failed to create temporary view: {e}")
        raise


def generate_profile_report(df: DataFrame, output_path: str = "data/profile_report.md") -> None:
    """
    Generate a simple data profile report (row count, nulls, min/max per column) and save as markdown.
    Args:
        df: Spark DataFrame to profile
        output_path: Path to save the markdown report
    """
    import pandas as pd
    logger = logging.getLogger(__name__)
    logger.info(f"Generating data profile report at {output_path}")
    profile = []
    row_count = df.count()
    profile.append(f"# Data Profile Report\n\n")
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


def transform_weather_data(input_path: str, output_path: str) -> None:
    """
    Main transformation function that orchestrates the entire process.
    
    Args:
        input_path: Path to input CSV file
        output_path: Path to save processed Parquet files
    """
    logger = logging.getLogger(__name__)
    
    spark = None
    try:
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
            spark.stop()


def main() -> None:
    """Main function to run the transformation process."""
    setup_logging()
    
    input_path = "data/landing/weather_data_*.csv"
    output_path = "data/processed/weather_parquet"
    
    transform_weather_data(input_path, output_path)


if __name__ == "__main__":
    main() 