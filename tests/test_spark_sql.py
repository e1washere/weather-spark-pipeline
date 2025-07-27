#!/usr/bin/env python3
"""
Unit tests for Spark SQL functionality and weather analytics.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum as spark_sum, max as spark_max, min as spark_min
from transform import create_spark_session


class TestWeatherAnalytics:
    """Test class for Spark SQL weather analytics."""
    
    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Setup method to create SparkSession for each test."""
        self.spark = create_spark_session("TestWeatherAnalytics")
        yield
        if self.spark:
            self.spark.stop()
    
    @pytest.mark.unit
    def test_temperature_conversion_unit(self):
        """Test temperature conversion using Spark SQL."""
        # Create test data
        data = [
            ("2024-01-01", "USW00014735", 250, 150, 10.0),  # 25.0°C, 15.0°C
            ("2024-01-02", "USW00014735", 300, 200, 5.0),   # 30.0°C, 20.0°C
            ("2024-01-03", "USW00014735", 200, 100, 0.0),   # 20.0°C, 10.0°C
        ]
        columns = ["date", "station", "tmax_raw", "tmin_raw", "prcp"]
        
        df = self.spark.createDataFrame(data, columns)
        df.createOrReplaceTempView("raw_weather")
        
        # Test temperature conversion
        result = self.spark.sql("""
            SELECT 
                date,
                station,
                tmax_raw / 10.0 as tmax_celsius,
                tmin_raw / 10.0 as tmin_celsius,
                (tmax_raw + tmin_raw) / 20.0 as tavg_celsius
            FROM raw_weather
            ORDER BY date
        """)
        
        results = result.collect()
        
        # Check conversions
        assert results[0].tmax_celsius == 25.0
        assert results[0].tmin_celsius == 15.0
        assert results[0].tavg_celsius == 20.0
        
        assert results[1].tmax_celsius == 30.0
        assert results[1].tmin_celsius == 20.0
        assert results[1].tavg_celsius == 25.0
    
    @pytest.mark.unit
    def test_precipitation_aggregation_unit(self):
        """Test precipitation aggregation using Spark SQL."""
        # Create test data
        data = [
            ("2024-01-01", "USW00014735", 10.0),
            ("2024-01-01", "USW00014735", 5.0),
            ("2024-01-02", "USW00014735", 0.0),
            ("2024-01-03", "USW00014735", 15.0),
        ]
        columns = ["date", "station", "prcp"]
        
        df = self.spark.createDataFrame(data, columns)
        df.createOrReplaceTempView("precipitation_data")
        
        # Test aggregation
        result = self.spark.sql("""
            SELECT 
                date,
                station,
                SUM(prcp) as total_precipitation,
                COUNT(*) as record_count
            FROM precipitation_data
            GROUP BY date, station
            ORDER BY date
        """)
        
        results = result.collect()
        
        # Check aggregations
        assert results[0].total_precipitation == 15.0  # 10.0 + 5.0
        assert results[0].record_count == 2
        
        assert results[1].total_precipitation == 0.0
        assert results[1].record_count == 1
        
        assert results[2].total_precipitation == 15.0
        assert results[2].record_count == 1
    
    @pytest.mark.unit
    def test_spark_sql_weather_analytics(self):
        """Test comprehensive weather analytics using Spark SQL."""
        # Create test data
        data = [
            ("2024-01-01", "USW00014735", 25.0, 15.0, 10.0),
            ("2024-01-02", "USW00014735", 30.0, 20.0, 5.0),
            ("2024-01-03", "USW00014735", 20.0, 10.0, 0.0),
            ("2024-01-04", "USW00014735", 35.0, 25.0, 20.0),
            ("2024-01-05", "USW00014735", 18.0, 8.0, 15.0),
        ]
        columns = ["date", "station", "tmax_celsius", "tmin_celsius", "prcp"]
        
        df = self.spark.createDataFrame(data, columns)
        df.createOrReplaceTempView("weather_data")
        
        # Test monthly averages
        monthly_query = """
            SELECT 
                SUBSTRING(date, 1, 7) as year_month,
                AVG(tmax_celsius) as avg_max_temp,
                AVG(tmin_celsius) as avg_min_temp,
                SUM(prcp) as total_precipitation
            FROM weather_data
            GROUP BY SUBSTRING(date, 1, 7)
            ORDER BY year_month
        """
        
        monthly_results = self.spark.sql(monthly_query).collect()
        assert len(monthly_results) == 1
        assert monthly_results[0].year_month == "2024-01"
        assert monthly_results[0].avg_max_temp == 25.6  # (25+30+20+35+18)/5
        assert monthly_results[0].total_precipitation == 50.0  # 10+5+0+20+15
        
        # Test temperature extremes
        extremes_query = """
            SELECT 
                date,
                tmax_celsius,
                tmin_celsius,
                CASE 
                    WHEN tmax_celsius > 30 THEN 'Hot'
                    WHEN tmax_celsius > 20 THEN 'Moderate'
                    ELSE 'Cold'
                END as temp_category
            FROM weather_data
            ORDER BY tmax_celsius DESC
        """
        
        extremes_results = self.spark.sql(extremes_query).collect()
        
        # Check temperature classification
        assert extremes_results[0]["temp_category"] == "Hot"       # 35°C
        assert extremes_results[1]["temp_category"] == "Moderate"  # 30°C
        assert extremes_results[2]["temp_category"] == "Moderate"  # 25°C
        assert extremes_results[3]["temp_category"] == "Cold"      # 20°C (изменил с Moderate на Cold)
        assert extremes_results[4]["temp_category"] == "Cold"      # 18°C
    
    @pytest.mark.unit
    def test_spark_sql_time_series_analysis(self):
        """Test time series analysis using Spark SQL window functions."""
        # Create test data
        data = [
            ("2024-01-01", "USW00014735", 25.0, 15.0),
            ("2024-01-02", "USW00014735", 30.0, 20.0),
            ("2024-01-03", "USW00014735", 20.0, 10.0),
            ("2024-01-04", "USW00014735", 35.0, 25.0),
            ("2024-01-05", "USW00014735", 18.0, 8.0),
        ]
        columns = ["date", "station", "tmax_celsius", "tmin_celsius"]
        
        df = self.spark.createDataFrame(data, columns)
        df.createOrReplaceTempView("time_series_data")
        
        # Test moving average
        moving_avg_query = """
            SELECT 
                date,
                tmax_celsius,
                AVG(tmax_celsius) OVER (
                    ORDER BY date 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as moving_avg_3day
            FROM time_series_data
            ORDER BY date
        """
        
        moving_avg_results = self.spark.sql(moving_avg_query).collect()
        
        # Check moving averages
        assert moving_avg_results[0].moving_avg_3day == 25.0  # Only one value
        assert moving_avg_results[1].moving_avg_3day == 27.5  # (25+30)/2
        assert moving_avg_results[2].moving_avg_3day == 25.0  # (25+30+20)/3
        
        # Test temperature trends
        trend_query = """
            SELECT 
                date,
                tmax_celsius,
                LAG(tmax_celsius, 1) OVER (ORDER BY date) as prev_day_temp,
                tmax_celsius - LAG(tmax_celsius, 1) OVER (ORDER BY date) as temp_change
            FROM time_series_data
            ORDER BY date
        """
        
        trend_results = self.spark.sql(trend_query).collect()
        
        # Check temperature changes
        assert trend_results[0].temp_change is None  # No previous day
        assert trend_results[1].temp_change == 5.0   # 30 - 25
        assert trend_results[2].temp_change == -10.0  # 20 - 30
    
    @pytest.mark.unit
    def test_data_quality_validation_unit(self):
        """Test data quality validation using Spark SQL."""
        # Create test data with some quality issues
        data = [
            ("2024-01-01", "USW00014735", 25.0, 15.0, 10.0),  # Normal
            ("2024-01-02", "USW00014735", 50.0, 20.0, 5.0),   # High temp
            ("2024-01-03", "USW00014735", 20.0, -10.0, 0.0),  # Low temp
            ("2024-01-04", "USW00014735", 30.0, 25.0, 100.0), # High precip
            ("2024-01-05", "USW00014735", 18.0, 8.0, 15.0),   # Normal
        ]
        columns = ["date", "station", "tmax_celsius", "tmin_celsius", "prcp"]
        
        df = self.spark.createDataFrame(data, columns)
        df.createOrReplaceTempView("quality_test_data")
        
        # Test data quality checks
        quality_query = """
            SELECT 
                date,
                tmax_celsius,
                tmin_celsius,
                prcp,
                CASE 
                    WHEN tmax_celsius > 40 OR tmin_celsius < -20 THEN 'Extreme Temperature'
                    WHEN tmax_celsius > 30 OR tmin_celsius < -10 THEN 'High Temperature'
                    ELSE 'Temperature OK'
                END as temp_quality,
                CASE 
                    WHEN prcp > 50 THEN 'Heavy Precipitation'
                    WHEN prcp > 20 THEN 'Moderate Precipitation'
                    ELSE 'Light Precipitation'
                END as precip_quality
            FROM quality_test_data
            ORDER BY date
        """
        
        quality_results = self.spark.sql(quality_query).collect()
        
        # Check quality classifications
        assert quality_results[0].temp_quality == "Temperature OK"
        assert quality_results[0].precip_quality == "Light Precipitation"
        
        assert quality_results[1].temp_quality == "Extreme Temperature"  # 50°C > 40°C
        assert quality_results[1].precip_quality == "Light Precipitation"
        
        assert quality_results[2].temp_quality == "Temperature OK"  # -10°C не попадает в условия High Temperature
        assert quality_results[2].precip_quality == "Light Precipitation"
        
        assert quality_results[3].temp_quality == "Temperature OK"
        assert quality_results[3].precip_quality == "Heavy Precipitation"
        
        assert quality_results[4].temp_quality == "Temperature OK"
        assert quality_results[4].precip_quality == "Light Precipitation" 