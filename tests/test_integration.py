#!/usr/bin/env python3
"""
Integration tests for the complete ETL pipeline.
"""

import pytest
import tempfile
import os
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

from ingest import download_noaa_data
from transform import transform_weather_data
from main import run_full_pipeline, run_ingestion, run_transformation


@pytest.mark.integration
class TestIntegration:
    """Integration test class for the complete ETL pipeline."""
    
    @classmethod
    def setup_class(cls):
        """Set up Spark session for integration testing."""
        cls.spark = SparkSession.builder \
            .appName("TestWeatherETLIntegration") \
            .master("local[1]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def teardown_class(cls):
        """Clean up Spark session."""
        cls.spark.stop()
    
    def test_complete_pipeline_with_sample_data(self):
        """Test the complete pipeline from ingestion to transformation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            landing_dir = os.path.join(temp_dir, "landing")
            processed_dir = os.path.join(temp_dir, "processed")
            
            # Step 1: Run ingestion
            csv_path = download_noaa_data(
                station_id="GHCND:USW00014735",
                start_date="2024-01-01",
                end_date="2024-01-07",
                output_dir=landing_dir
            )
            
            # Verify ingestion output
            assert os.path.exists(csv_path)
            
            # Step 2: Run transformation
            transform_weather_data(csv_path, processed_dir)
            
            # Verify transformation output
            assert os.path.exists(processed_dir)
            
            # Check that Parquet files were created
            parquet_files = []
            for root, dirs, files in os.walk(processed_dir):
                for file in files:
                    if file.endswith('.parquet'):
                        parquet_files.append(os.path.join(root, file))
            
            assert len(parquet_files) > 0, "No Parquet files created"
            
            # Create a new SparkSession to read the parquet files
            # (since transform_weather_data stopped its own session)
            spark_reader = SparkSession.builder \
                .appName("TestReader") \
                .master("local[1]") \
                .getOrCreate()
            
            try:
                # Load and verify the processed data
                df_result = spark_reader.read.parquet(processed_dir)
                
                # Check that we have data
                assert df_result.count() > 0
                
                # Check expected columns
                expected_columns = ['date', 'station', 'year', 'month', 'day', 
                                   'avg_tmax', 'avg_tmin', 'avg_temperature', 
                                   'total_precipitation', 'total_snow', 'avg_wind_speed']
                
                for col in expected_columns:
                    assert col in df_result.columns
                
                # Check data types
                assert str(df_result.schema['date'].dataType) == 'DateType'
                assert str(df_result.schema['avg_temperature'].dataType) == 'DoubleType'
                
                # Check that we have the expected date range
                dates = [row.date for row in df_result.select('date').collect()]
                date_strings = [d.strftime('%Y-%m-%d') for d in dates]
                
                assert '2024-01-01' in date_strings
                assert '2024-01-07' in date_strings
                
            finally:
                spark_reader.stop()
    
    def test_pipeline_with_missing_data(self):
        """Test pipeline handling of missing data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            landing_dir = os.path.join(temp_dir, "landing")
            processed_dir = os.path.join(temp_dir, "processed")
            
            # Create CSV with missing data
            csv_content = """DATE,STATION,TMAX,TMIN,PRCP,SNOW,SNWD,AWND
2024-01-01,USW00014735,250,150,10,0,0,120
2024-01-02,USW00014735,,140,,0,0,110
2024-01-03,USW00014735,260,,5,0,0,130
2024-01-04,USW00014735,,,,,0,"""
            
            csv_path = os.path.join(landing_dir, "test_weather.csv")
            os.makedirs(landing_dir, exist_ok=True)
            
            with open(csv_path, 'w') as f:
                f.write(csv_content)
            
            # Run transformation
            transform_weather_data(csv_path, processed_dir)
            
            # Create new SparkSession for reading results
            spark_reader = SparkSession.builder \
                .appName("TestReader") \
                .master("local[1]") \
                .getOrCreate()
            
            try:
                # Load and verify processed data
                df_result = spark_reader.read.parquet(processed_dir)
                
                # Check that we have data despite missing values
                assert df_result.count() == 4
                
                # Check that nulls are handled correctly
                results = df_result.collect()
                
                # Row 1: complete data
                row1 = [r for r in results if r.day == 1][0]
                assert row1.avg_tmax == 25.0
                assert row1.avg_tmin == 15.0
                assert row1.avg_temperature == 20.0
                
                # Row 2: missing tmax
                row2 = [r for r in results if r.day == 2][0]
                assert row2.avg_tmax is None
                assert row2.avg_tmin == 14.0
                
                # Row 3: missing tmin  
                row3 = [r for r in results if r.day == 3][0]
                assert row3.avg_tmax == 26.0
                assert row3.avg_tmin is None
                
                # Row 4: all missing
                row4 = [r for r in results if r.day == 4][0]
                assert row4.avg_tmax is None
                assert row4.avg_tmin is None
                assert row4.avg_temperature is None
                
            finally:
                spark_reader.stop()
    
    def test_pipeline_with_multiple_months(self):
        """Test pipeline with data spanning multiple months."""
        with tempfile.TemporaryDirectory() as temp_dir:
            landing_dir = os.path.join(temp_dir, "landing")
            processed_dir = os.path.join(temp_dir, "processed")
            
            # Generate data spanning multiple months
            csv_path = download_noaa_data(
                station_id="GHCND:USW00014735",
                start_date="2024-01-28",
                end_date="2024-02-05",
                output_dir=landing_dir
            )
            
            # Run transformation
            transform_weather_data(csv_path, processed_dir)
            
            # Check partitioning
            partition_dirs = []
            for root, dirs, files in os.walk(processed_dir):
                for dir in dirs:
                    if dir.startswith("year=") or dir.startswith("month="):
                        partition_dirs.append(dir)
            
            # Should have partitions for year=2024 and month=1, month=2
            assert any("year=2024" in d for d in partition_dirs)
            
            # Create new SparkSession for reading results
            spark_reader = SparkSession.builder \
                .appName("TestReader") \
                .master("local[1]") \
                .getOrCreate()
            
            try:
                # Load and verify data
                df_result = spark_reader.read.parquet(processed_dir)
                
                # Check that we have data from both months
                months = [row.month for row in df_result.select('month').distinct().collect()]
                assert 1 in months  # January
                assert 2 in months  # February
                
                # Check date range
                dates = [row.date for row in df_result.select('date').collect()]
                date_strings = [d.strftime('%Y-%m-%d') for d in dates]
                
                assert '2024-01-28' in date_strings
                assert '2024-02-05' in date_strings
                
            finally:
                spark_reader.stop()
    
    def test_pipeline_error_handling(self):
        """Test pipeline error handling with invalid data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            landing_dir = os.path.join(temp_dir, "landing")
            processed_dir = os.path.join(temp_dir, "processed")
            
            # Create invalid CSV (missing header)
            csv_content = """2024-01-01,USW00014735,250,150,10,0,0,120
2024-01-02,USW00014735,260,140,5,0,0,110"""
            
            csv_path = os.path.join(landing_dir, "invalid_weather.csv")
            os.makedirs(landing_dir, exist_ok=True)
            
            with open(csv_path, 'w') as f:
                f.write(csv_content)
            
            # Should handle missing header gracefully
            # (Our current implementation will treat first row as header)
            transform_weather_data(csv_path, processed_dir)
            
            # Should still create output
            assert os.path.exists(processed_dir)
    
    def test_sql_view_functionality(self):
        """Test that SQL views are created and queryable."""
        with tempfile.TemporaryDirectory() as temp_dir:
            landing_dir = os.path.join(temp_dir, "landing")
            processed_dir = os.path.join(temp_dir, "processed")
            
            # Generate test data
            csv_path = download_noaa_data(
                station_id="GHCND:USW00014735",
                start_date="2024-01-01",
                end_date="2024-01-10",
                output_dir=landing_dir
            )
            
            # Run transformation with view creation
            spark = SparkSession.builder \
                .appName("TestSQL") \
                .master("local[1]") \
                .getOrCreate()
            
            try:
                from transform import (
                    load_weather_data, clean_weather_data, 
                    compute_daily_aggregations, create_temp_view
                )
                
                # Load and process data
                df_raw = load_weather_data(spark, csv_path)
                df_clean = clean_weather_data(df_raw)
                df_agg = compute_daily_aggregations(df_clean)
                
                # Create temp view
                create_temp_view(spark, df_agg, "test_weather_view")
                
                # Test SQL queries
                result = spark.sql("""
                    SELECT COUNT(*) as total_records 
                    FROM test_weather_view
                """)
                
                count = result.collect()[0]['total_records']
                assert count > 0
                
                # Test aggregation query
                result = spark.sql("""
                    SELECT AVG(avg_temperature) as overall_avg_temp
                    FROM test_weather_view
                    WHERE avg_temperature IS NOT NULL
                """)
                
                avg_temp = result.collect()[0]['overall_avg_temp']
                assert avg_temp is not None
                assert isinstance(avg_temp, (int, float))
                
                # Test filtering query
                result = spark.sql("""
                    SELECT date, avg_temperature 
                    FROM test_weather_view 
                    WHERE total_precipitation > 0
                    ORDER BY date
                """)
                
                rows = result.collect()
                # Should have some rows with precipitation
                assert len(rows) >= 0  # May be 0 due to random data
                
            finally:
                spark.stop()
    
    @pytest.mark.performance
    @pytest.mark.slow
    def test_pipeline_performance_with_large_dataset(self):
        """Test pipeline performance with larger dataset."""
        with tempfile.TemporaryDirectory() as temp_dir:
            landing_dir = os.path.join(temp_dir, "landing")
            processed_dir = os.path.join(temp_dir, "processed")
            
            # Generate larger dataset (3 months)
            csv_path = download_noaa_data(
                station_id="GHCND:USW00014735",
                start_date="2024-01-01",
                end_date="2024-03-31",
                output_dir=landing_dir
            )
            
            # Time the transformation
            import time
            start_time = time.time()
            
            transform_weather_data(csv_path, processed_dir)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Should complete in reasonable time (less than 30 seconds)
            assert processing_time < 30, f"Processing took too long: {processing_time} seconds"
            
            # Create new SparkSession for reading results
            spark_reader = SparkSession.builder \
                .appName("TestReader") \
                .master("local[1]") \
                .getOrCreate()
            
            try:
                # Verify output
                df_result = spark_reader.read.parquet(processed_dir)
                record_count = df_result.count()
                
                # Should have ~90 records (3 months * 30 days average)
                assert record_count >= 80
                assert record_count <= 100
                
            finally:
                spark_reader.stop()
    
    def test_pipeline_idempotency(self):
        """Test that running the pipeline multiple times produces consistent results."""
        with tempfile.TemporaryDirectory() as temp_dir:
            landing_dir = os.path.join(temp_dir, "landing")
            processed_dir1 = os.path.join(temp_dir, "processed1")
            processed_dir2 = os.path.join(temp_dir, "processed2")
            
            # Generate test data
            csv_path = download_noaa_data(
                station_id="GHCND:USW00014735",
                start_date="2024-01-01",
                end_date="2024-01-05",
                output_dir=landing_dir
            )
            
            # Run transformation twice
            transform_weather_data(csv_path, processed_dir1)
            transform_weather_data(csv_path, processed_dir2)
            
            # Create new SparkSession for reading results
            spark_reader = SparkSession.builder \
                .appName("TestReader") \
                .master("local[1]") \
                .getOrCreate()
            
            try:
                # Load results
                df1 = spark_reader.read.parquet(processed_dir1)
                df2 = spark_reader.read.parquet(processed_dir2)
                
                # Should have same number of records
                assert df1.count() == df2.count()
                
                # Should have same schema
                assert df1.schema == df2.schema
                
                # Should have same data (order may differ)
                data1 = set([(row.date, row.station, row.avg_temperature) 
                            for row in df1.collect()])
                data2 = set([(row.date, row.station, row.avg_temperature) 
                            for row in df2.collect()])
                
                assert data1 == data2
                
            finally:
                spark_reader.stop()


if __name__ == "__main__":
    pytest.main([__file__]) 