#!/usr/bin/env python3
"""
Tests for data validation module.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

from src.service.validation import (
    validate_data_schema,
    validate_data_ranges,
    validate_date_format,
    calculate_data_quality_score,
    validate_weather_data
)


@pytest.fixture(scope="function")
def spark_session():
    """Create SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("ValidationTest") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def valid_weather_data(spark_session):
    """Create valid weather data for testing."""
    data = [
        ("2024-01-01", "GHCND:USW00014735", "25", "15", "0", "0", "0", "50"),
        ("2024-01-02", "GHCND:USW00014735", "30", "20", "50", "0", "0", "60"),
        ("2024-01-03", "GHCND:USW00014735", "28", "18", "25", "0", "0", "45")
    ]
    
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
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def invalid_weather_data(spark_session):
    """Create invalid weather data for testing."""
    data = [
        ("2024-01-01", "GHCND:USW00014735", "999", "-999", "9999", "999", "999", "999"),
        ("invalid-date", "GHCND:USW00014735", "", "", "", "", "", ""),
        ("2024-01-03", "GHCND:USW00014735", "280", "180", "25", "0", "0", "45")
    ]
    
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
    
    return spark_session.createDataFrame(data, schema)


class TestDataValidation:
    """Test data validation functions."""
    
    @pytest.mark.unit
    def test_validate_data_schema_valid(self, valid_weather_data):
        """Test schema validation with valid data."""
        is_valid, issues = validate_data_schema(valid_weather_data)
        
        assert is_valid is True
        assert len(issues) == 0
    
    @pytest.mark.unit
    def test_validate_data_schema_missing_columns(self, spark_session):
        """Test schema validation with missing required columns."""
        data = [("2024-01-01", "250", "150")]  # Missing STATION
        schema = StructType([
            StructField("DATE", StringType(), True),
            StructField("TMAX", StringType(), True),
            StructField("TMIN", StringType(), True)
        ])
        df = spark_session.createDataFrame(data, schema)
        
        is_valid, issues = validate_data_schema(df)
        
        assert is_valid is False
        assert "STATION" in str(issues)
    
    @pytest.mark.unit
    def test_validate_data_ranges_valid(self, valid_weather_data):
        """Test range validation with valid data."""
        is_valid, issues = validate_data_ranges(valid_weather_data)
        
        assert is_valid is True
        assert len(issues) == 0
    
    @pytest.mark.unit
    def test_validate_data_ranges_invalid(self, invalid_weather_data):
        """Test range validation with invalid data."""
        is_valid, issues = validate_data_ranges(invalid_weather_data)
        
        assert is_valid is False
        assert len(issues) > 0
    
    @pytest.mark.unit
    def test_validate_date_format_valid(self, valid_weather_data):
        """Test date format validation with valid data."""
        is_valid, issues = validate_date_format(valid_weather_data)
        
        assert is_valid is True
        assert len(issues) == 0
    
    @pytest.mark.unit
    def test_validate_date_format_invalid(self, invalid_weather_data):
        """Test date format validation with invalid data."""
        is_valid, issues = validate_date_format(invalid_weather_data)
        
        assert is_valid is False
        assert len(issues) > 0
    
    @pytest.mark.unit
    def test_calculate_data_quality_score_perfect(self, valid_weather_data):
        """Test quality score calculation with perfect data."""
        score = calculate_data_quality_score(valid_weather_data)
        
        assert score == 1.0
    
    @pytest.mark.unit
    def test_calculate_data_quality_score_with_nulls(self, spark_session):
        """Test quality score calculation with null values."""
        data = [
            ("2024-01-01", "GHCND:USW00014735", "25", "15", "0", "0", "0", "50"),
            ("2024-01-02", "GHCND:USW00014735", None, None, "50", "0", "0", "60"),  # Nulls in required columns
            ("2024-01-03", "GHCND:USW00014735", "28", "18", "25", "0", "0", "45")
        ]
        
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
        
        df = spark_session.createDataFrame(data, schema)
        score = calculate_data_quality_score(df)
        
        assert 0.0 < score < 1.0
    
    @pytest.mark.unit
    def test_calculate_data_quality_score_empty(self, spark_session):
        """Test quality score calculation with empty DataFrame."""
        data = []
        schema = StructType([
            StructField("DATE", StringType(), True),
            StructField("STATION", StringType(), True),
            StructField("TMAX", StringType(), True),
            StructField("TMIN", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        score = calculate_data_quality_score(df)
        
        assert score == 0.0
    
    @pytest.mark.integration
    def test_validate_weather_data_comprehensive_valid(self, valid_weather_data):
        """Test comprehensive validation with valid data."""
        results = validate_weather_data(valid_weather_data)
        
        assert results["overall_valid"] is True
        assert results["schema_valid"] is True
        assert results["ranges_valid"] is True
        assert results["dates_valid"] is True
        assert results["quality_score"] == 1.0
        assert len(results["issues"]) == 0
    
    @pytest.mark.integration
    def test_validate_weather_data_comprehensive_invalid(self, invalid_weather_data):
        """Test comprehensive validation with invalid data."""
        results = validate_weather_data(invalid_weather_data)
        
        assert results["overall_valid"] is False
        assert len(results["issues"]) > 0
        assert results["quality_score"] < 1.0
    
    @pytest.mark.unit
    def test_validate_data_schema_wrong_types(self, spark_session):
        """Test schema validation with wrong data types."""
        data = [("2024-01-01", "GHCND:USW00014735", 25.0, 15.0)]  # Numeric instead of string
        schema = StructType([
            StructField("DATE", StringType(), True),
            StructField("STATION", StringType(), True),
            StructField("TMAX", DoubleType(), True),  # Wrong type
            StructField("TMIN", DoubleType(), True)   # Wrong type
        ])
        df = spark_session.createDataFrame(data, schema)
        
        is_valid, issues = validate_data_schema(df)
        
        assert is_valid is False
        assert "StringType" in str(issues)
    
    @pytest.mark.unit
    def test_validate_data_ranges_edge_cases(self, spark_session):
        """Test range validation with edge case values."""
        data = [
            ("2024-01-01", "GHCND:USW00014735", "-50", "-50", "0", "0", "0", "0"),  # Min values
            ("2024-01-02", "GHCND:USW00014735", "60", "60", "1000", "500", "500", "100")  # Max values
        ]
        
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
        
        df = spark_session.createDataFrame(data, schema)
        is_valid, issues = validate_data_ranges(df)
        
        assert is_valid is True
        assert len(issues) == 0 