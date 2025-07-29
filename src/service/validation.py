#!/usr/bin/env python3
"""
Data Validation Module

Handles data quality checks and validation for weather data processing.
"""

import logging
from typing import Dict, List, Tuple, Any
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, count, isnan, isnull, min as spark_min, max as spark_max

from ..config import config


def validate_data_schema(df: DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame schema against expected weather data schema.
    
    Args:
        df: Spark DataFrame to validate
        
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    logger = logging.getLogger(__name__)
    issues = []
    
    expected_columns = config.VALIDATION_RULES["required_columns"]
    actual_columns = df.columns
    
    # Check required columns
    missing_columns = set(expected_columns) - set(actual_columns)
    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")
    
    # Check data types for numeric columns
    for col_name in config.NUMERIC_COLUMNS:
        if col_name in actual_columns:
            col_type = str(df.schema[col_name].dataType)
            if "String" not in col_type:
                issues.append(f"Column {col_name} should be StringType, got {col_type}")
    
    is_valid = len(issues) == 0
    if not is_valid:
        logger.warning(f"Schema validation failed: {issues}")
    else:
        logger.info("Schema validation passed")
    
    return is_valid, issues


def validate_data_ranges(df: DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate data ranges for numeric columns.
    
    Args:
        df: Spark DataFrame to validate
        
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    logger = logging.getLogger(__name__)
    issues = []
    
    # Convert string columns to numeric for validation
    temp_df = df
    
    for col_name in config.NUMERIC_COLUMNS:
        if col_name in df.columns:
            # Convert to numeric, handling empty strings
            temp_df = temp_df.withColumn(
                f"{col_name}_numeric",
                when(col(col_name) == "", None)
                .otherwise(col(col_name).cast("double"))
            )
    
    # Check ranges for each numeric column
    range_checks = {
        "TMAX": config.VALIDATION_RULES["temperature_range"],
        "TMIN": config.VALIDATION_RULES["temperature_range"],
        "PRCP": config.VALIDATION_RULES["precipitation_range"],
        "SNOW": config.VALIDATION_RULES["snow_range"],
        "AWND": config.VALIDATION_RULES["wind_range"]
    }
    
    for col_name, range_config in range_checks.items():
        if f"{col_name}_numeric" in temp_df.columns:
            # Get min and max values
            stats = temp_df.select(
                spark_min(f"{col_name}_numeric").alias("min_val"),
                spark_max(f"{col_name}_numeric").alias("max_val")
            ).collect()[0]
            
            min_val = stats["min_val"]
            max_val = stats["max_val"]
            
            if min_val is not None and min_val < range_config["min"]:
                issues.append(f"{col_name} minimum value {min_val} below threshold {range_config['min']}")
            
            if max_val is not None and max_val > range_config["max"]:
                issues.append(f"{col_name} maximum value {max_val} above threshold {range_config['max']}")
    
    is_valid = len(issues) == 0
    if not is_valid:
        logger.warning(f"Data range validation failed: {issues}")
    else:
        logger.info("Data range validation passed")
    
    return is_valid, issues


def validate_date_format(df: DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate date format in DATE column.
    
    Args:
        df: Spark DataFrame to validate
        
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    logger = logging.getLogger(__name__)
    issues = []
    
    if "DATE" not in df.columns:
        issues.append("DATE column not found")
        return False, issues
    
    # Count invalid dates
    invalid_dates = df.filter(
        col("DATE").isNull() | 
        (col("DATE") == "") |
        ~col("DATE").rlike(r"^\d{4}-\d{2}-\d{2}$")
    ).count()
    
    total_dates = df.filter(col("DATE").isNotNull()).count()
    
    if total_dates > 0:
        invalid_percentage = (invalid_dates / total_dates) * 100
        if invalid_percentage > 5:  # More than 5% invalid dates
            issues.append(f"High percentage of invalid dates: {invalid_percentage:.2f}%")
    
    is_valid = len(issues) == 0
    if not is_valid:
        logger.warning(f"Date format validation failed: {issues}")
    else:
        logger.info("Date format validation passed")
    
    return is_valid, issues


def calculate_data_quality_score(df: DataFrame) -> float:
    """
    Calculate overall data quality score.
    
    Args:
        df: Spark DataFrame to evaluate
        
    Returns:
        Data quality score between 0.0 and 1.0
    """
    logger = logging.getLogger(__name__)
    
    total_records = df.count()
    if total_records == 0:
        return 0.0
    
    # Count null values in required columns
    null_counts = {}
    for col_name in config.VALIDATION_RULES["required_columns"]:
        if col_name in df.columns:
            null_count = df.filter(col(col_name).isNull() | (col(col_name) == "")).count()
            null_counts[col_name] = null_count
    
    # Calculate quality score
    total_null_records = sum(null_counts.values())
    quality_score = 1.0 - (total_null_records / (total_records * len(config.VALIDATION_RULES["required_columns"])))
    
    logger.info(f"Data quality score: {quality_score:.3f}")
    return quality_score


def validate_weather_data(df: DataFrame) -> Dict[str, Any]:
    """
    Comprehensive data validation for weather data.
    
    Args:
        df: Spark DataFrame to validate
        
    Returns:
        Dictionary with validation results
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting comprehensive data validation")
    
    validation_results = {
        "overall_valid": True,
        "schema_valid": False,
        "ranges_valid": False,
        "dates_valid": False,
        "quality_score": 0.0,
        "issues": []
    }
    
    # Schema validation
    schema_valid, schema_issues = validate_data_schema(df)
    validation_results["schema_valid"] = schema_valid
    validation_results["issues"].extend(schema_issues)
    
    # Range validation
    ranges_valid, range_issues = validate_data_ranges(df)
    validation_results["ranges_valid"] = ranges_valid
    validation_results["issues"].extend(range_issues)
    
    # Date validation
    dates_valid, date_issues = validate_date_format(df)
    validation_results["dates_valid"] = dates_valid
    validation_results["issues"].extend(date_issues)
    
    # Quality score
    quality_score = calculate_data_quality_score(df)
    validation_results["quality_score"] = quality_score
    
    # Overall validation
    validation_results["overall_valid"] = (
        schema_valid and 
        ranges_valid and 
        dates_valid and 
        quality_score >= config.PERFORMANCE_THRESHOLDS["min_data_quality_score"]
    )
    
    if validation_results["overall_valid"]:
        logger.info("Data validation passed")
    else:
        logger.warning(f"Data validation failed: {validation_results['issues']}")
    
    return validation_results 