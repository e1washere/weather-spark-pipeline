#!/usr/bin/env python3
"""
Unit tests for the transform module.
"""

import pytest
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
from src.service import (
    create_spark_session, clean_weather_data, compute_daily_aggregations,
    create_temp_view, load_weather_data, save_processed_data
)


class TestTransform:
    """Test class for transform module functions."""
    
    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Setup method to create SparkSession for each test."""
        self.spark = create_spark_session("TestTransform")
        yield
        if self.spark:
            self.spark.stop()
    
    @pytest.mark.unit
    def test_create_spark_session(self):
        """Test SparkSession creation."""
        spark = create_spark_session("TestApp")
        assert spark is not None
        assert spark.sparkContext is not None
        spark.stop()
    
    @pytest.mark.unit
    def test_clean_weather_data(self):
        """Test weather data cleaning functionality."""
        # Create test data
        raw_data = [
            ("2024-01-01", "USW00014735", "250", "150", "10", "0", "0", "50"),
            ("2024-01-02", "USW00014735", "", "160", "0", "0", "0", "60"),
            ("2024-01-03", "USW00014735", "260", "", "20", "5", "10", "70")
        ]
        columns = ["DATE", "STATION", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        
        df_raw = self.spark.createDataFrame(raw_data, columns)
        df_clean = clean_weather_data(df_raw)
        
        # Check that data was cleaned
        assert df_clean.count() == 3
        assert "tmax_celsius" in df_clean.columns
        assert "tmin_celsius" in df_clean.columns
        assert "tavg_celsius" in df_clean.columns
        
        # Check temperature conversion (250/10 = 25.0)
        result = df_clean.filter(df_clean.date == "2024-01-01").first()
        assert result.tmax_celsius == 25.0
        assert result.tmin_celsius == 15.0
        assert result.tavg_celsius == 20.0
    
    @pytest.mark.unit
    def test_compute_daily_aggregations(self):
        """Test daily aggregations computation."""
        # Create cleaned data
        clean_data = [
            ("2024-01-01", "USW00014735", 2024, 1, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 0.0, 50.0),
            ("2024-01-01", "USW00014735", 2024, 1, 1, 26.0, 16.0, 21.0, 5.0, 0.0, 0.0, 55.0),
            ("2024-01-02", "USW00014735", 2024, 1, 2, 27.0, 17.0, 22.0, 15.0, 0.0, 0.0, 60.0)
        ]
        columns = ["date", "station", "year", "month", "day", "tmax_celsius", 
                  "tmin_celsius", "tavg_celsius", "prcp", "snow", "snwd", "awnd"]
        
        df_clean = self.spark.createDataFrame(clean_data, columns)
        df_agg = compute_daily_aggregations(df_clean)
        
        # Check aggregations
        assert df_agg.count() == 2  # 2 unique dates
        
        # Check first day aggregations
        day1 = df_agg.filter(df_agg.date == "2024-01-01").first()
        assert day1.avg_temperature == 20.5  # (20.0 + 21.0) / 2
        assert day1.total_precipitation == 15.0  # 10.0 + 5.0
    
    @pytest.mark.unit
    def test_create_temp_view(self):
        """Test temporary view creation."""
        # Create test data
        data = [
            ("2024-01-01", "USW00014735", 20.5, 15.0),
            ("2024-01-02", "USW00014735", 22.0, 10.0)
        ]
        columns = ["date", "station", "avg_temperature", "total_precipitation"]
        
        df = self.spark.createDataFrame(data, columns)
        create_temp_view(self.spark, df, "test_weather")
        
        # Check if view exists
        result = self.spark.sql("SELECT COUNT(*) as count FROM test_weather").collect()
        assert result[0]["count"] == 2  # Исправил доступ к полю
    
    @pytest.mark.unit
    def test_clean_weather_data_with_nulls(self):
        """Test cleaning with null values."""
        raw_data = [
            ("2024-01-01", "USW00014735", "250", "150", "10", "0", "0", "50"),
            ("2024-01-02", "USW00014735", "", "", "0", "0", "0", "60"),
            ("2024-01-03", "USW00014735", "260", "160", "", "5", "10", "70")
        ]
        columns = ["DATE", "STATION", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        
        df_raw = self.spark.createDataFrame(raw_data, columns)
        df_clean = clean_weather_data(df_raw)
        
        # Check that nulls are handled properly
        null_count = df_clean.filter(df_clean.tmax_celsius.isNull()).count()
        assert null_count == 1  # One row with empty TMAX
    
    @pytest.mark.unit
    def test_aggregations_with_nulls(self):
        """Test aggregations with null values."""
        clean_data = [
            ("2024-01-01", "USW00014735", 2024, 1, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 0.0, 50.0),
            ("2024-01-01", "USW00014735", 2024, 1, 1, None, 16.0, None, 5.0, 0.0, 0.0, 55.0),
            ("2024-01-02", "USW00014735", 2024, 1, 2, 27.0, None, None, 15.0, 0.0, 0.0, 60.0)
        ]
        columns = ["date", "station", "year", "month", "day", "tmax_celsius", 
                  "tmin_celsius", "tavg_celsius", "prcp", "snow", "snwd", "awnd"]
        
        df_clean = self.spark.createDataFrame(clean_data, columns)
        df_agg = compute_daily_aggregations(df_clean)
        
        # Should still compute aggregations for non-null values
        assert df_agg.count() == 2
    
    @pytest.mark.unit
    def test_load_weather_data_with_schema(self):
        """Test loading weather data with schema."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("DATE,STATION,TMAX,TMIN,PRCP,SNOW,SNWD,AWND\n")
            f.write("2024-01-01,USW00014735,250,150,10,0,0,50\n")
            f.write("2024-01-02,USW00014735,260,160,0,0,0,60\n")
            temp_file = f.name
        
        try:
            df = load_weather_data(self.spark, temp_file)
            assert df.count() == 2
            assert "DATE" in df.columns
            assert "STATION" in df.columns
        finally:
            os.unlink(temp_file)
    
    @pytest.mark.unit
    def test_clean_weather_data_temperature_conversion(self):
        """Test temperature conversion from tenths to degrees."""
        raw_data = [
            ("2024-01-01", "USW00014735", "300", "200", "10", "0", "0", "50"),  # 30.0°C, 20.0°C
            ("2024-01-02", "USW00014735", "250", "150", "0", "0", "0", "60"),  # 25.0°C, 15.0°C
        ]
        columns = ["DATE", "STATION", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        
        df_raw = self.spark.createDataFrame(raw_data, columns)
        df_clean = clean_weather_data(df_raw)
        
        # Check temperature conversion
        result = df_clean.filter(df_clean.date == "2024-01-01").first()
        assert result.tmax_celsius == 30.0  # 300/10
        assert result.tmin_celsius == 20.0  # 200/10
        assert result.tavg_celsius == 25.0  # (30+20)/2
    
    @pytest.mark.unit
    def test_multiple_stations_aggregation(self):
        """Test aggregation with multiple stations."""
        clean_data = [
            ("2024-01-01", "USW00014735", 2024, 1, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 0.0, 50.0),
            ("2024-01-01", "USW00014736", 2024, 1, 1, 26.0, 16.0, 21.0, 5.0, 0.0, 0.0, 55.0),
            ("2024-01-02", "USW00014735", 2024, 1, 2, 27.0, 17.0, 22.0, 15.0, 0.0, 0.0, 60.0)
        ]
        columns = ["date", "station", "year", "month", "day", "tmax_celsius", 
                  "tmin_celsius", "tavg_celsius", "prcp", "snow", "snwd", "awnd"]
        
        df_clean = self.spark.createDataFrame(clean_data, columns)
        df_agg = compute_daily_aggregations(df_clean)
        
        # Should have 3 rows (2 dates x 2 stations, but one station missing on day 2)
        assert df_agg.count() == 3
    
    @pytest.mark.unit
    def test_save_and_load_parquet(self):
        """Test saving and loading Parquet files."""
        data = [
            ("2024-01-01", "USW00014735", 25.0, 15.0, 20.0, 10.0, 0.0, 0.0, 50.0),
            ("2024-01-02", "USW00014735", 26.0, 16.0, 21.0, 5.0, 0.0, 0.0, 55.0)
        ]
        columns = ["date", "station", "tmax_celsius", "tmin_celsius", "tavg_celsius", 
                  "prcp", "snow", "snwd", "awnd"]
        
        df = self.spark.createDataFrame(data, columns)
        
        # Save to temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "test_parquet")
            # Сохраняем без партиционирования для теста
            df.write.mode("overwrite").parquet(output_path)
            
            # Load back and verify
            df_loaded = self.spark.read.parquet(output_path)
            assert df_loaded.count() == 2
            # Проверяем только основные колонки, так как партиционирование может изменить порядок
            assert "date" in df_loaded.columns
            assert "station" in df_loaded.columns
            assert "tmax_celsius" in df_loaded.columns
    
    @pytest.mark.unit
    def test_edge_cases_with_extreme_values(self):
        """Test edge cases with extreme temperature values."""
        raw_data = [
            ("2024-01-01", "USW00014735", "500", "100", "10", "0", "0", "50"),  # 50.0°C, 10.0°C
            ("2024-01-02", "USW00014735", "100", "500", "0", "0", "0", "60"),  # 10.0°C, 50.0°C
        ]
        columns = ["DATE", "STATION", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        
        df_raw = self.spark.createDataFrame(raw_data, columns)
        df_clean = clean_weather_data(df_raw)
        
        # Check extreme values are handled
        result1 = df_clean.filter(df_clean.date == "2024-01-01").first()
        assert result1.tmax_celsius == 50.0
        assert result1.tmin_celsius == 10.0
        assert result1.tavg_celsius == 30.0  # (50+10)/2 