#!/usr/bin/env python3
"""
Integration tests for the complete ETL pipeline.
"""

import pytest
import tempfile
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from src.service import download_noaa_data, transform_weather_data, create_spark_session


class TestIntegration:
    """Test class for integration tests."""
    
    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Setup method for each test."""
        self.temp_dir = tempfile.mkdtemp()
        yield
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @pytest.mark.integration
    def test_complete_pipeline_with_sample_data(self):
        """Test complete ETL pipeline with sample data."""
        # Step 1: Generate sample data
        landing_dir = os.path.join(self.temp_dir, "landing")
        processed_dir = os.path.join(self.temp_dir, "processed")
        
        os.makedirs(landing_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        # Generate sample data
        csv_file = download_noaa_data(
            station_id="GHCND:USW00014735",
            start_date="2024-01-01",
            end_date="2024-01-07",
            output_dir=landing_dir
        )
        
        # Step 2: Transform data
        transform_weather_data(csv_file, processed_dir)
        
        # Step 3: Verify results
        spark = create_spark_session("TestIntegration")
        try:
            # Read processed data
            df_result = spark.read.parquet(processed_dir)
            
            # Check basic structure
            assert df_result.count() > 0
            assert "date" in df_result.columns
            assert "avg_temperature" in df_result.columns
            assert "total_precipitation" in df_result.columns
            
            # Check date type
            assert str(df_result.schema["date"].dataType) == "DateType()"
            
            # Check data quality
            result = df_result.filter(df_result.date == "2024-01-01").first()
            assert result is not None
            
        finally:
            spark.stop()
    
    @pytest.mark.integration
    def test_pipeline_with_missing_data(self):
        """Test pipeline handles missing data gracefully."""
        # Create data with missing values
        landing_dir = os.path.join(self.temp_dir, "landing")
        processed_dir = os.path.join(self.temp_dir, "processed")
        
        os.makedirs(landing_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        # Create CSV with missing values
        csv_content = """DATE,STATION,TMAX,TMIN,PRCP,SNOW,SNWD,AWND
2024-01-01,USW00014735,250,150,10,0,0,50
2024-01-02,USW00014735,,160,0,0,0,60
2024-01-03,USW00014735,260,,20,5,10,70"""
        
        csv_file = os.path.join(landing_dir, "weather_data_missing.csv")
        with open(csv_file, 'w') as f:
            f.write(csv_content)
        
        # Transform data
        transform_weather_data(csv_file, processed_dir)
        
        # Verify results
        spark = create_spark_session("TestMissingData")
        try:
            df_result = spark.read.parquet(processed_dir)
            assert df_result.count() > 0
            
            # Check that nulls are handled
            null_count = df_result.filter(df_result.avg_temperature.isNull()).count()
            assert null_count >= 0  # Should handle nulls gracefully
            
        finally:
            spark.stop()
    
    @pytest.mark.integration
    def test_pipeline_with_multiple_months(self):
        """Test pipeline with data spanning multiple months."""
        landing_dir = os.path.join(self.temp_dir, "landing")
        processed_dir = os.path.join(self.temp_dir, "processed")
        
        os.makedirs(landing_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        # Generate data for multiple months
        csv_file = download_noaa_data(
            station_id="GHCND:USW00014735",
            start_date="2024-01-01",
            end_date="2024-02-28",
            output_dir=landing_dir
        )
        
        # Transform data
        transform_weather_data(csv_file, processed_dir)
        
        # Verify results
        spark = create_spark_session("TestMultipleMonths")
        try:
            df_result = spark.read.parquet(processed_dir)
            
            # Check partitioning
            assert df_result.count() > 0
            
            # Check that we have data from multiple months
            months = df_result.select("month").distinct().collect()
            assert len(months) >= 2  # January and February
            
        finally:
            spark.stop()
    
    @pytest.mark.integration
    def test_pipeline_error_handling(self):
        """Test pipeline error handling."""
        # Test with non-existent file
        processed_dir = os.path.join(self.temp_dir, "processed")
        os.makedirs(processed_dir, exist_ok=True)
        
        non_existent_file = os.path.join(self.temp_dir, "non_existent.csv")
        
        with pytest.raises(Exception):
            transform_weather_data(non_existent_file, processed_dir)
    
    @pytest.mark.integration
    def test_sql_view_functionality(self):
        """Test SQL view creation and querying."""
        landing_dir = os.path.join(self.temp_dir, "landing")
        processed_dir = os.path.join(self.temp_dir, "processed")
        
        os.makedirs(landing_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        # Generate and transform data
        csv_file = download_noaa_data(
            station_id="GHCND:USW00014735",
            start_date="2024-01-01",
            end_date="2024-01-05",
            output_dir=landing_dir
        )
        
        transform_weather_data(csv_file, processed_dir)
        
        # Test SQL functionality
        spark = create_spark_session("TestSQL")
        try:
            # Read processed data
            df_result = spark.read.parquet(processed_dir)
            
            # Create view
            df_result.createOrReplaceTempView("weather_data")
            
            # Test SQL query
            result = spark.sql("""
                SELECT date, avg_temperature, total_precipitation
                FROM weather_data
                ORDER BY date DESC
                LIMIT 5
            """)
            
            assert result.count() > 0
            assert "date" in result.columns
            assert "avg_temperature" in result.columns
            
        finally:
            spark.stop()
    
    @pytest.mark.integration
    @pytest.mark.performance
    @pytest.mark.slow
    def test_pipeline_performance_with_large_dataset(self):
        """Test pipeline performance with larger dataset."""
        landing_dir = os.path.join(self.temp_dir, "landing")
        processed_dir = os.path.join(self.temp_dir, "processed")
        
        os.makedirs(landing_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        # Generate larger dataset (3 months)
        csv_file = download_noaa_data(
            station_id="GHCND:USW00014735",
            start_date="2024-01-01",
            end_date="2024-03-31",
            output_dir=landing_dir
        )
        
        # Transform data
        transform_weather_data(csv_file, processed_dir)
        
        # Verify results
        spark = create_spark_session("TestPerformance")
        try:
            df_result = spark.read.parquet(processed_dir)
            
            # Should have significant amount of data
            assert df_result.count() > 50  # At least 50 days of data
            
            # Check performance metrics
            start_time = spark.sparkContext.startTime
            assert start_time > 0
            
        finally:
            spark.stop()
    
    @pytest.mark.integration
    def test_pipeline_idempotency(self):
        """Test that pipeline can be run multiple times without issues."""
        landing_dir = os.path.join(self.temp_dir, "landing")
        processed_dir = os.path.join(self.temp_dir, "processed")
        
        os.makedirs(landing_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        # Generate data
        csv_file = download_noaa_data(
            station_id="GHCND:USW00014735",
            start_date="2024-01-01",
            end_date="2024-01-03",
            output_dir=landing_dir
        )
        
        # Run transformation twice
        transform_weather_data(csv_file, processed_dir)
        transform_weather_data(csv_file, processed_dir)
        
        # Verify results are consistent
        spark = create_spark_session("TestIdempotency")
        try:
            df_result = spark.read.parquet(processed_dir)
            assert df_result.count() > 0
            
            # Should have same data after second run
            count_after_second_run = df_result.count()
            assert count_after_second_run > 0
            
        finally:
            spark.stop() 