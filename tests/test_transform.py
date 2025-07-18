#!/usr/bin/env python3
"""
Unit tests for the transform module.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

from transform import (
    create_spark_session, 
    clean_weather_data, 
    compute_daily_aggregations,
    create_temp_view
)


class TestTransform:
    """Test class for transformation functions."""
    
    @classmethod
    def setup_class(cls):
        """Set up Spark session for testing."""
        cls.spark = SparkSession.builder \
            .appName("TestWeatherETL") \
            .master("local[1]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def teardown_class(cls):
        """Clean up Spark session."""
        cls.spark.stop()
    
    def test_create_spark_session(self):
        """Test Spark session creation."""
        spark = create_spark_session("TestApp")
        
        assert spark is not None
        assert spark.sparkContext.appName == "TestApp"
        
        spark.stop()
    
    def test_clean_weather_data(self):
        """Test weather data cleaning function."""
        # Create sample raw data
        raw_data = [
            ("2024-01-01", "USW00014735", "250", "150", "10", "0", "0", "120"),
            ("2024-01-02", "USW00014735", "", "140", "", "0", "0", "110"),
            ("2024-01-03", "USW00014735", "260", "", "5", "0", "0", "130")
        ]
        
        columns = ["DATE", "STATION", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        
        df_raw = self.spark.createDataFrame(raw_data, columns)
        
        # Clean the data
        df_clean = clean_weather_data(df_raw)
        
        # Check that date column is properly converted
        assert "date" in df_clean.columns
        assert df_clean.schema["date"].dataType == DateType()
        
        # Check that temperature columns are created
        assert "tmax_celsius" in df_clean.columns
        assert "tmin_celsius" in df_clean.columns
        assert "tavg_celsius" in df_clean.columns
        
        # Check that year, month, day columns are added
        assert "year" in df_clean.columns
        assert "month" in df_clean.columns  
        assert "day" in df_clean.columns
        
        # Collect results for validation
        results = df_clean.collect()
        
        # Check first row has correct temperature conversion (250 -> 25.0)
        assert results[0]["tmax_celsius"] == 25.0
        assert results[0]["tmin_celsius"] == 15.0
        assert results[0]["tavg_celsius"] == 20.0
        
        # Check second row has null for missing tmax
        assert results[1]["tmax_celsius"] is None
        assert results[1]["tmin_celsius"] == 14.0
        
        # Check third row has null for missing tmin
        assert results[2]["tmin_celsius"] is None
        assert results[2]["tmax_celsius"] == 26.0
    
    def test_compute_daily_aggregations(self):
        """Test daily aggregations computation."""
        # Create sample clean data
        from datetime import date
        
        clean_data = [
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 0.0, 120.0),
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 26.0, 16.0, 21.0, 5.0, 0.0, 0.0, 110.0),
            (date(2024, 1, 2), "USW00014735", 2024, 1, 2, 22.0, 12.0, 17.0, 0.0, 0.0, 0.0, 100.0)
        ]
        
        columns = ["date", "station", "year", "month", "day", "tmax_celsius", 
                  "tmin_celsius", "tavg_celsius", "prcp", "snow", "snwd", "awnd"]
        
        df_clean = self.spark.createDataFrame(clean_data, columns)
        
        # Compute aggregations
        df_agg = compute_daily_aggregations(df_clean)
        
        # Check columns exist
        expected_columns = ["date", "station", "year", "month", "day", "avg_tmax", 
                           "avg_tmin", "avg_temperature", "total_precipitation", 
                           "total_snow", "avg_wind_speed"]
        
        for col in expected_columns:
            assert col in df_agg.columns
        
        # Collect results
        results = df_agg.collect()
        
        # Should have 2 rows (2 unique dates)
        assert len(results) == 2
        
        # Check aggregation for first date (2 records)
        jan_1_data = [r for r in results if r["date"] == date(2024, 1, 1)][0]
        assert jan_1_data["avg_tmax"] == 25.5  # (25.0 + 26.0) / 2
        assert jan_1_data["avg_tmin"] == 15.5  # (15.0 + 16.0) / 2
        assert jan_1_data["total_precipitation"] == 15.0  # 10.0 + 5.0
        
        # Check aggregation for second date (1 record)
        jan_2_data = [r for r in results if r["date"] == date(2024, 1, 2)][0]
        assert jan_2_data["avg_tmax"] == 22.0
        assert jan_2_data["avg_tmin"] == 12.0
        assert jan_2_data["total_precipitation"] == 0.0
    
    def test_create_temp_view(self):
        """Test temporary view creation."""
        from datetime import date
        
        # Create sample data
        data = [
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 120.0),
            (date(2024, 1, 2), "USW00014735", 2024, 1, 2, 22.0, 12.0, 17.0, 0.0, 0.0, 100.0)
        ]
        
        columns = ["date", "station", "year", "month", "day", "avg_tmax", 
                  "avg_tmin", "avg_temperature", "total_precipitation", 
                  "total_snow", "avg_wind_speed"]
        
        df = self.spark.createDataFrame(data, columns)
        
        # Create temporary view
        view_name = "test_weather_view"
        create_temp_view(self.spark, df, view_name)
        
        # Test that view exists by querying it
        result = self.spark.sql(f"SELECT COUNT(*) as count FROM {view_name}")
        count = result.collect()[0]["count"]
        
        assert count == 2
        
        # Test a more complex query
        result = self.spark.sql(f"""
            SELECT date, avg_temperature 
            FROM {view_name} 
            WHERE avg_temperature > 18
            ORDER BY date
        """)
        
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["avg_temperature"] == 20.0
    
    def test_clean_weather_data_with_nulls(self):
        """Test cleaning data with various null/empty values."""
        # Create sample data with various null representations
        raw_data = [
            ("2024-01-01", "USW00014735", "250", "150", "10", "0", "0", "120"),
            ("2024-01-02", "USW00014735", "", "", "", "", "", ""),
            ("2024-01-03", "USW00014735", "260", "140", "5", "0", "0", "130"),
            (None, "USW00014735", "270", "160", "15", "0", "0", "140")
        ]
        
        columns = ["DATE", "STATION", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        
        df_raw = self.spark.createDataFrame(raw_data, columns)
        
        # Clean the data
        df_clean = clean_weather_data(df_raw)
        
        # Filter out null dates for testing
        df_clean = df_clean.filter(df_clean.date.isNotNull())
        
        results = df_clean.collect()
        
        # Check that empty strings are converted to null
        row_2 = [r for r in results if r["day"] == 2][0]
        assert row_2["tmax_celsius"] is None
        assert row_2["tmin_celsius"] is None
        assert row_2["prcp"] is None
        assert row_2["tavg_celsius"] is None  # Should be null when both temps are null
    
    def test_aggregations_with_nulls(self):
        """Test aggregations handle null values correctly."""
        from datetime import date
        
        clean_data = [
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 0.0, 120.0),
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, None, None, None, None, 0.0, 0.0, 110.0),
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 27.0, 17.0, 22.0, 5.0, 0.0, 0.0, 130.0)
        ]
        
        columns = ["date", "station", "year", "month", "day", "tmax_celsius", 
                  "tmin_celsius", "tavg_celsius", "prcp", "snow", "snwd", "awnd"]
        
        df_clean = self.spark.createDataFrame(clean_data, columns)
        
        # Compute aggregations
        df_agg = compute_daily_aggregations(df_clean)
        
        results = df_agg.collect()
        
        # Should have 1 row for the single date
        assert len(results) == 1
        
        # Check that null values are handled correctly in aggregations
        result = results[0]
        assert result["avg_tmax"] == 26.0  # (25.0 + 27.0) / 2, null ignored
        assert result["avg_tmin"] == 16.0  # (15.0 + 17.0) / 2, null ignored
        assert result["total_precipitation"] == 15.0  # 10.0 + 5.0, null treated as 0


if __name__ == "__main__":
    pytest.main([__file__]) 