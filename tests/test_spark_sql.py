#!/usr/bin/env python3
"""
Focused unit tests with Spark SQL examples for the weather ETL pipeline.
"""

import pytest
import tempfile
import os
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

from transform import create_spark_session, clean_weather_data, compute_daily_aggregations
from ingest import generate_sample_weather_data


@pytest.mark.unit
class TestWeatherAnalytics:
    """Focused unit tests for weather analytics with Spark SQL examples."""
    
    @classmethod
    def setup_class(cls):
        """Set up Spark session for testing."""
        cls.spark = create_spark_session("TestWeatherAnalytics")
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def teardown_class(cls):
        """Clean up Spark session."""
        cls.spark.stop()
    
    def test_temperature_conversion_unit(self):
        """Unit test for temperature conversion from tenths of degrees to Celsius."""
        # Test data with known temperature values
        raw_data = [
            ("2024-01-01", "USW00014735", "250", "150", "10", "0", "0", "120"),  # 25.0°C, 15.0°C
            ("2024-01-02", "USW00014735", "0", "-100", "0", "0", "0", "100"),   # 0.0°C, -10.0°C
            ("2024-01-03", "USW00014735", "350", "200", "20", "0", "0", "130")  # 35.0°C, 20.0°C
        ]
        
        columns = ["DATE", "STATION", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        df_raw = self.spark.createDataFrame(raw_data, columns)
        
        # Apply temperature conversion
        df_clean = clean_weather_data(df_raw)
        
        # Verify specific temperature conversions
        results = df_clean.select("date", "tmax_celsius", "tmin_celsius", "tavg_celsius").collect()
        
        # Test Case 1: 25.0°C max, 15.0°C min -> 20.0°C average
        assert results[0]["tmax_celsius"] == 25.0
        assert results[0]["tmin_celsius"] == 15.0
        assert results[0]["tavg_celsius"] == 20.0
        
        # Test Case 2: 0.0°C max, -10.0°C min -> -5.0°C average
        assert results[1]["tmax_celsius"] == 0.0
        assert results[1]["tmin_celsius"] == -10.0
        assert results[1]["tavg_celsius"] == -5.0
        
        # Test Case 3: 35.0°C max, 20.0°C min -> 27.5°C average
        assert results[2]["tmax_celsius"] == 35.0
        assert results[2]["tmin_celsius"] == 20.0
        assert results[2]["tavg_celsius"] == 27.5
    
    def test_precipitation_aggregation_unit(self):
        """Unit test for precipitation aggregation logic."""
        # Test data with known precipitation values
        clean_data = [
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 0.0, 120.0),
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 27.0, 17.0, 22.0, 5.0, 0.0, 0.0, 130.0),
            (date(2024, 1, 2), "USW00014735", 2024, 1, 2, 20.0, 10.0, 15.0, 0.0, 0.0, 0.0, 100.0)
        ]
        
        columns = ["date", "station", "year", "month", "day", "tmax_celsius", 
                  "tmin_celsius", "tavg_celsius", "prcp", "snow", "snwd", "awnd"]
        
        df_clean = self.spark.createDataFrame(clean_data, columns)
        
        # Apply aggregation
        df_agg = compute_daily_aggregations(df_clean)
        
        # Verify precipitation aggregation
        results = df_agg.select("date", "total_precipitation").collect()
        
        # Jan 1: 10.0 + 5.0 = 15.0 mm total precipitation
        jan_1_result = [r for r in results if r["date"] == date(2024, 1, 1)][0]
        assert jan_1_result["total_precipitation"] == 15.0
        
        # Jan 2: 0.0 mm total precipitation
        jan_2_result = [r for r in results if r["date"] == date(2024, 1, 2)][0]
        assert jan_2_result["total_precipitation"] == 0.0
    
    def test_spark_sql_weather_analytics(self):
        """Test Spark SQL analytics queries on weather data."""
        # Create sample weather data
        weather_data = [
            (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 120.0),
            (date(2024, 1, 2), "USW00014735", 2024, 1, 2, 30.0, 18.0, 24.0, 0.0, 0.0, 110.0),
            (date(2024, 1, 3), "USW00014735", 2024, 1, 3, 28.0, 16.0, 22.0, 5.0, 0.0, 130.0),
            (date(2024, 2, 1), "USW00014735", 2024, 2, 1, 15.0, 5.0, 10.0, 20.0, 0.0, 140.0),
            (date(2024, 2, 2), "USW00014735", 2024, 2, 2, 18.0, 8.0, 13.0, 15.0, 0.0, 125.0),
        ]
        
        schema = StructType([
            StructField("date", DateType(), True),
            StructField("station", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("avg_tmax", DoubleType(), True),
            StructField("avg_tmin", DoubleType(), True),
            StructField("avg_temperature", DoubleType(), True),
            StructField("total_precipitation", DoubleType(), True),
            StructField("total_snow", DoubleType(), True),
            StructField("avg_wind_speed", DoubleType(), True),
        ])
        
        df = self.spark.createDataFrame(weather_data, schema)
        df.createOrReplaceTempView("weather_analytics")
        
        # Test Query 1: Monthly temperature averages
        monthly_avg_query = """
            SELECT 
                year, 
                month,
                ROUND(AVG(avg_temperature), 2) as monthly_avg_temp,
                ROUND(SUM(total_precipitation), 1) as monthly_precipitation,
                COUNT(*) as days_count
            FROM weather_analytics 
            GROUP BY year, month
            ORDER BY year, month
        """
        
        monthly_results = self.spark.sql(monthly_avg_query).collect()
        
        # Verify January results: (20+24+22)/3 = 22.0°C, 10+0+5 = 15.0mm
        jan_result = monthly_results[0]
        assert jan_result["year"] == 2024
        assert jan_result["month"] == 1
        assert jan_result["monthly_avg_temp"] == 22.0
        assert jan_result["monthly_precipitation"] == 15.0
        assert jan_result["days_count"] == 3
        
        # Verify February results: (10+13)/2 = 11.5°C, 20+15 = 35.0mm
        feb_result = monthly_results[1]
        assert feb_result["year"] == 2024
        assert feb_result["month"] == 2
        assert feb_result["monthly_avg_temp"] == 11.5
        assert feb_result["monthly_precipitation"] == 35.0
        assert feb_result["days_count"] == 2
        
        # Test Query 2: Weather extremes classification
        extremes_query = """
            SELECT 
                date,
                avg_temperature,
                total_precipitation,
                CASE 
                    WHEN avg_temperature > 25 THEN 'Hot'
                    WHEN avg_temperature < 15 THEN 'Cold'
                    ELSE 'Moderate'
                END as temp_category,
                CASE 
                    WHEN total_precipitation > 10 THEN 'Heavy Rain'
                    WHEN total_precipitation > 0 THEN 'Light Rain'
                    ELSE 'No Rain'
                END as rain_category
            FROM weather_analytics
            ORDER BY date
        """
        
        extremes_results = self.spark.sql(extremes_query).collect()
        
        # Verify temperature classifications
        assert extremes_results[0]["temp_category"] == "Moderate"  # 20°C
        assert extremes_results[1]["temp_category"] == "Hot"       # 24°C
        assert extremes_results[2]["temp_category"] == "Hot"       # 22°C
        assert extremes_results[3]["temp_category"] == "Cold"      # 10°C
        assert extremes_results[4]["temp_category"] == "Cold"      # 13°C
        
        # Verify precipitation classifications
        assert extremes_results[0]["rain_category"] == "Light Rain"  # 10mm
        assert extremes_results[1]["rain_category"] == "No Rain"     # 0mm
        assert extremes_results[2]["rain_category"] == "Light Rain"  # 5mm
        assert extremes_results[3]["rain_category"] == "Heavy Rain"  # 20mm
        assert extremes_results[4]["rain_category"] == "Heavy Rain"  # 15mm
        
        # Test Query 3: Statistical summary
        stats_query = """
            SELECT 
                COUNT(*) as total_records,
                ROUND(AVG(avg_temperature), 2) as overall_avg_temp,
                ROUND(MIN(avg_temperature), 2) as min_temp,
                ROUND(MAX(avg_temperature), 2) as max_temp,
                ROUND(SUM(total_precipitation), 1) as total_precipitation,
                ROUND(AVG(avg_wind_speed), 1) as avg_wind_speed
            FROM weather_analytics
        """
        
        stats_result = self.spark.sql(stats_query).collect()[0]
        
        # Verify statistical calculations
        assert stats_result["total_records"] == 5
        assert stats_result["overall_avg_temp"] == 17.8  # (20+24+22+10+13)/5
        assert stats_result["min_temp"] == 10.0
        assert stats_result["max_temp"] == 24.0
        assert stats_result["total_precipitation"] == 50.0  # 10+0+5+20+15
        assert stats_result["avg_wind_speed"] == 125.0  # (120+110+130+140+125)/5
    
    def test_spark_sql_time_series_analysis(self):
        """Test Spark SQL time series analysis with window functions."""
        # Create time series data
        time_series_data = [
            (date(2024, 1, 1), "USW00014735", 20.0, 10.0),
            (date(2024, 1, 2), "USW00014735", 22.0, 5.0),
            (date(2024, 1, 3), "USW00014735", 25.0, 0.0),
            (date(2024, 1, 4), "USW00014735", 23.0, 15.0),
            (date(2024, 1, 5), "USW00014735", 21.0, 8.0),
        ]
        
        schema = StructType([
            StructField("date", DateType(), True),
            StructField("station", StringType(), True),
            StructField("avg_temperature", DoubleType(), True),
            StructField("total_precipitation", DoubleType(), True),
        ])
        
        df = self.spark.createDataFrame(time_series_data, schema)
        df.createOrReplaceTempView("time_series_weather")
        
        # Test window function analysis
        window_query = """
            SELECT 
                date,
                avg_temperature,
                total_precipitation,
                LAG(avg_temperature, 1) OVER (ORDER BY date) as prev_day_temp,
                LEAD(avg_temperature, 1) OVER (ORDER BY date) as next_day_temp,
                AVG(avg_temperature) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_3day_avg_temp,
                SUM(total_precipitation) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_3day_precip
            FROM time_series_weather
            ORDER BY date
        """
        
        window_results = self.spark.sql(window_query).collect()
        
        # Test lag and lead functions
        assert window_results[1]["prev_day_temp"] == 20.0  # Jan 2 previous day
        assert window_results[1]["next_day_temp"] == 25.0  # Jan 2 next day
        
        # Test rolling averages (3-day window)
        # Jan 3: (20+22+25)/3 = 22.33...
        jan_3_rolling = round(window_results[2]["rolling_3day_avg_temp"], 2)
        assert jan_3_rolling == 22.33
        
        # Jan 4: (22+25+23)/3 = 23.33...
        jan_4_rolling = round(window_results[3]["rolling_3day_avg_temp"], 2)
        assert jan_4_rolling == 23.33
        
        # Test rolling precipitation sums
        # Jan 3: 10+5+0 = 15.0
        assert window_results[2]["rolling_3day_precip"] == 15.0
        
        # Jan 4: 5+0+15 = 20.0
        assert window_results[3]["rolling_3day_precip"] == 20.0
    
    def test_data_quality_validation_unit(self):
        """Unit test for data quality validation using Spark SQL."""
        # Create test data with quality issues
        quality_data = [
            (date(2024, 1, 1), "USW00014735", 25.0, 15.0, 20.0, 10.0),  # Good data
            (date(2024, 1, 2), "USW00014735", None, None, None, 0.0),   # Missing temperatures
            (date(2024, 1, 3), "USW00014735", 35.0, 20.0, 27.5, -5.0),  # Negative precipitation (invalid)
            (date(2024, 1, 4), "USW00014735", 50.0, 45.0, 47.5, 100.0), # Extreme temperatures
        ]
        
        schema = StructType([
            StructField("date", DateType(), True),
            StructField("station", StringType(), True),
            StructField("avg_tmax", DoubleType(), True),
            StructField("avg_tmin", DoubleType(), True),
            StructField("avg_temperature", DoubleType(), True),
            StructField("total_precipitation", DoubleType(), True),
        ])
        
        df = self.spark.createDataFrame(quality_data, schema)
        df.createOrReplaceTempView("quality_check")
        
        # Data quality validation query
        quality_query = """
            SELECT 
                date,
                station,
                avg_temperature,
                total_precipitation,
                CASE 
                    WHEN avg_temperature IS NULL THEN 'Missing Temperature'
                    WHEN avg_temperature < -50 OR avg_temperature > 60 THEN 'Extreme Temperature'
                    ELSE 'Temperature OK'
                END as temp_quality,
                CASE 
                    WHEN total_precipitation < 0 THEN 'Invalid Precipitation'
                    WHEN total_precipitation > 500 THEN 'Extreme Precipitation'
                    ELSE 'Precipitation OK'
                END as precip_quality,
                CASE 
                    WHEN avg_temperature IS NULL OR total_precipitation < 0 THEN 'Poor'
                    WHEN avg_temperature < -50 OR avg_temperature > 60 OR total_precipitation > 500 THEN 'Questionable'
                    ELSE 'Good'
                END as overall_quality
            FROM quality_check
            ORDER BY date
        """
        
        quality_results = self.spark.sql(quality_query).collect()
        
        # Verify quality assessments
        assert quality_results[0]["temp_quality"] == "Temperature OK"
        assert quality_results[0]["precip_quality"] == "Precipitation OK"
        assert quality_results[0]["overall_quality"] == "Good"
        
        assert quality_results[1]["temp_quality"] == "Missing Temperature"
        assert quality_results[1]["overall_quality"] == "Poor"
        
        assert quality_results[2]["precip_quality"] == "Invalid Precipitation"
        assert quality_results[2]["overall_quality"] == "Poor"
        
        assert quality_results[3]["temp_quality"] == "Extreme Temperature"
        assert quality_results[3]["overall_quality"] == "Questionable"
        
        # Count quality distribution
        quality_summary_query = """
            SELECT 
                overall_quality,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
            FROM (
                SELECT 
                    CASE 
                        WHEN avg_temperature IS NULL OR total_precipitation < 0 THEN 'Poor'
                        WHEN avg_temperature < -50 OR avg_temperature > 60 OR total_precipitation > 500 THEN 'Questionable'
                        ELSE 'Good'
                    END as overall_quality
                FROM quality_check
            )
            GROUP BY overall_quality
            ORDER BY overall_quality
        """
        
        quality_summary = self.spark.sql(quality_summary_query).collect()
        
        # Verify quality distribution: 1 Good, 2 Poor, 1 Questionable
        good_quality = [r for r in quality_summary if r["overall_quality"] == "Good"][0]
        poor_quality = [r for r in quality_summary if r["overall_quality"] == "Poor"][0]
        questionable_quality = [r for r in quality_summary if r["overall_quality"] == "Questionable"][0]
        
        assert good_quality["count"] == 1
        assert poor_quality["count"] == 2
        assert questionable_quality["count"] == 1
        
        assert good_quality["percentage"] == 25.0
        assert poor_quality["percentage"] == 50.0
        assert questionable_quality["percentage"] == 25.0


if __name__ == "__main__":
    pytest.main([__file__]) 