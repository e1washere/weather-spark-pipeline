#!/usr/bin/env python3
"""
Spark SQL Analytics Example for Weather Data

This script demonstrates how to use Spark SQL for weather data analytics.
Shows practical examples of queries, aggregations, and time series analysis.
"""

import os
import sys
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, count, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform import create_spark_session


def create_sample_weather_data(spark: SparkSession):
    """Create comprehensive sample weather data for demonstration."""
    
    # Generate sample data for 3 months with multiple weather patterns
    weather_data = []
    
    # January - Winter data
    january_data = [
        (date(2024, 1, 1), "USW00014735", 2024, 1, 1, 5.0, -2.0, 1.5, 0.0, 5.0, 80.0),
        (date(2024, 1, 2), "USW00014735", 2024, 1, 2, 3.0, -5.0, -1.0, 0.0, 8.0, 90.0),
        (date(2024, 1, 3), "USW00014735", 2024, 1, 3, 8.0, 1.0, 4.5, 2.0, 0.0, 70.0),
        (date(2024, 1, 4), "USW00014735", 2024, 1, 4, 10.0, 3.0, 6.5, 0.0, 0.0, 60.0),
        (date(2024, 1, 5), "USW00014735", 2024, 1, 5, 2.0, -8.0, -3.0, 0.0, 10.0, 100.0),
        (date(2024, 1, 6), "USW00014735", 2024, 1, 6, 7.0, 0.0, 3.5, 5.0, 0.0, 85.0),
        (date(2024, 1, 7), "USW00014735", 2024, 1, 7, 12.0, 4.0, 8.0, 0.0, 0.0, 65.0),
    ]
    
    # February - Late winter/early spring
    february_data = [
        (date(2024, 2, 1), "USW00014735", 2024, 2, 1, 15.0, 5.0, 10.0, 8.0, 0.0, 90.0),
        (date(2024, 2, 2), "USW00014735", 2024, 2, 2, 18.0, 8.0, 13.0, 12.0, 0.0, 75.0),
        (date(2024, 2, 3), "USW00014735", 2024, 2, 3, 20.0, 10.0, 15.0, 0.0, 0.0, 60.0),
        (date(2024, 2, 4), "USW00014735", 2024, 2, 4, 22.0, 12.0, 17.0, 0.0, 0.0, 55.0),
        (date(2024, 2, 5), "USW00014735", 2024, 2, 5, 16.0, 6.0, 11.0, 15.0, 0.0, 80.0),
        (date(2024, 2, 6), "USW00014735", 2024, 2, 6, 19.0, 9.0, 14.0, 3.0, 0.0, 70.0),
        (date(2024, 2, 7), "USW00014735", 2024, 2, 7, 25.0, 15.0, 20.0, 0.0, 0.0, 45.0),
    ]
    
    # March - Spring data
    march_data = [
        (date(2024, 3, 1), "USW00014735", 2024, 3, 1, 25.0, 15.0, 20.0, 10.0, 0.0, 85.0),
        (date(2024, 3, 2), "USW00014735", 2024, 3, 2, 28.0, 18.0, 23.0, 0.0, 0.0, 60.0),
        (date(2024, 3, 3), "USW00014735", 2024, 3, 3, 30.0, 20.0, 25.0, 0.0, 0.0, 50.0),
        (date(2024, 3, 4), "USW00014735", 2024, 3, 4, 32.0, 22.0, 27.0, 0.0, 0.0, 45.0),
        (date(2024, 3, 5), "USW00014735", 2024, 3, 5, 27.0, 17.0, 22.0, 25.0, 0.0, 95.0),
        (date(2024, 3, 6), "USW00014735", 2024, 3, 6, 24.0, 14.0, 19.0, 18.0, 0.0, 90.0),
        (date(2024, 3, 7), "USW00014735", 2024, 3, 7, 26.0, 16.0, 21.0, 5.0, 0.0, 75.0),
    ]
    
    # Add data for second weather station for comparison
    station2_data = [
        (date(2024, 1, 1), "USW00012345", 2024, 1, 1, 15.0, 8.0, 11.5, 0.0, 0.0, 70.0),
        (date(2024, 1, 2), "USW00012345", 2024, 1, 2, 18.0, 10.0, 14.0, 2.0, 0.0, 65.0),
        (date(2024, 2, 1), "USW00012345", 2024, 2, 1, 22.0, 12.0, 17.0, 8.0, 0.0, 80.0),
        (date(2024, 2, 2), "USW00012345", 2024, 2, 2, 25.0, 15.0, 20.0, 0.0, 0.0, 60.0),
        (date(2024, 3, 1), "USW00012345", 2024, 3, 1, 28.0, 18.0, 23.0, 5.0, 0.0, 75.0),
        (date(2024, 3, 2), "USW00012345", 2024, 3, 2, 30.0, 20.0, 25.0, 0.0, 0.0, 55.0),
    ]
    
    # Combine all data
    all_data = january_data + february_data + march_data + station2_data
    
    # Define schema
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
    
    # Create DataFrame
    df = spark.createDataFrame(all_data, schema)
    
    return df


def demonstrate_basic_sql_queries(spark: SparkSession, df):
    """Demonstrate basic SQL queries and aggregations."""
    print("=" * 60)
    print("1. BASIC SQL QUERIES AND AGGREGATIONS")
    print("=" * 60)
    
    # Create temporary view
    df.createOrReplaceTempView("weather_data")
    
    # Query 1: Monthly averages
    print("\nMonthly Weather Averages:")
    print("-" * 40)
    
    monthly_query = """
        SELECT 
            year,
            month,
            CASE 
                WHEN month = 1 THEN 'January'
                WHEN month = 2 THEN 'February'
                WHEN month = 3 THEN 'March'
                ELSE 'Other'
            END as month_name,
            COUNT(*) as days_count,
            ROUND(AVG(avg_temperature), 1) as avg_temp_celsius,
            ROUND(SUM(total_precipitation), 1) as total_precipitation_mm,
            ROUND(AVG(avg_wind_speed), 1) as avg_wind_speed
        FROM weather_data
        GROUP BY year, month
        ORDER BY year, month
    """
    
    spark.sql(monthly_query).show()
    
    # Query 2: Temperature extremes
    print("\nTemperature Extremes:")
    print("-" * 40)
    
    extremes_query = """
        SELECT 
            date,
            station,
            avg_temperature,
            CASE 
                WHEN avg_temperature >= 25 THEN 'Hot'
                WHEN avg_temperature >= 15 THEN 'Warm'
                WHEN avg_temperature >= 5 THEN 'Cool'
                WHEN avg_temperature >= 0 THEN 'Cold'
                ELSE 'Freezing'
            END as temperature_category
        FROM weather_data
        WHERE avg_temperature >= 25 OR avg_temperature < 0
        ORDER BY avg_temperature DESC
    """
    
    spark.sql(extremes_query).show()
    
    # Query 3: Precipitation analysis
    print("\nPrecipitation Analysis:")
    print("-" * 40)
    
    precipitation_query = """
        SELECT 
            month,
            COUNT(*) as total_days,
            SUM(CASE WHEN total_precipitation > 0 THEN 1 ELSE 0 END) as rainy_days,
            ROUND(SUM(CASE WHEN total_precipitation > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rainy_day_percentage,
            ROUND(AVG(total_precipitation), 1) as avg_daily_precipitation,
            ROUND(MAX(total_precipitation), 1) as max_daily_precipitation
        FROM weather_data
        GROUP BY month
        ORDER BY month
    """
    
    spark.sql(precipitation_query).show()


def demonstrate_advanced_sql_analytics(spark: SparkSession, df):
    """Demonstrate advanced SQL analytics with window functions and CTEs."""
    print("=" * 60)
    print("2. ADVANCED SQL ANALYTICS")
    print("=" * 60)
    
    # Query 1: Time series analysis with window functions
    print("\nTime Series Analysis with Window Functions:")
    print("-" * 50)
    
    time_series_query = """
        SELECT 
            date,
            station,
            avg_temperature,
            total_precipitation,
            LAG(avg_temperature, 1) OVER (PARTITION BY station ORDER BY date) as prev_day_temp,
            LEAD(avg_temperature, 1) OVER (PARTITION BY station ORDER BY date) as next_day_temp,
            avg_temperature - LAG(avg_temperature, 1) OVER (PARTITION BY station ORDER BY date) as temp_change,
            AVG(avg_temperature) OVER (PARTITION BY station ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_3day_avg,
            SUM(total_precipitation) OVER (PARTITION BY station ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_precip
        FROM weather_data
        WHERE station = 'USW00014735'
        ORDER BY date
        LIMIT 10
    """
    
    spark.sql(time_series_query).show()
    
    # Query 2: Station comparison with ranking
    print("\nStation Comparison with Ranking:")
    print("-" * 40)
    
    station_comparison_query = """
        WITH monthly_stats AS (
            SELECT 
                station,
                month,
                AVG(avg_temperature) as monthly_avg_temp,
                SUM(total_precipitation) as monthly_total_precip
            FROM weather_data
            GROUP BY station, month
        )
        SELECT 
            station,
            month,
            ROUND(monthly_avg_temp, 1) as avg_temp,
            ROUND(monthly_total_precip, 1) as total_precip,
            RANK() OVER (PARTITION BY month ORDER BY monthly_avg_temp DESC) as temp_rank,
            RANK() OVER (PARTITION BY month ORDER BY monthly_total_precip DESC) as precip_rank
        FROM monthly_stats
        ORDER BY month, temp_rank
    """
    
    spark.sql(station_comparison_query).show()
    
    # Query 3: Weather pattern analysis
    print("\nWeather Pattern Analysis:")
    print("-" * 40)
    
    pattern_query = """
        WITH daily_patterns AS (
            SELECT 
                date,
                station,
                avg_temperature,
                total_precipitation,
                avg_wind_speed,
                CASE 
                    WHEN avg_temperature >= 25 AND total_precipitation = 0 THEN 'Hot & Dry'
                    WHEN avg_temperature >= 15 AND total_precipitation > 10 THEN 'Warm & Rainy'
                    WHEN avg_temperature < 5 AND total_snow > 0 THEN 'Cold & Snowy'
                    WHEN avg_temperature < 0 THEN 'Freezing'
                    WHEN total_precipitation > 15 THEN 'Heavy Rain'
                    ELSE 'Normal'
                END as weather_pattern
            FROM weather_data
        )
        SELECT 
            weather_pattern,
            COUNT(*) as occurrences,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
            ROUND(AVG(avg_temperature), 1) as avg_temp_for_pattern,
            ROUND(AVG(total_precipitation), 1) as avg_precip_for_pattern
        FROM daily_patterns
        GROUP BY weather_pattern
        ORDER BY occurrences DESC
    """
    
    spark.sql(pattern_query).show()


def demonstrate_data_quality_analysis(spark: SparkSession, df):
    """Demonstrate data quality analysis using SQL."""
    print("=" * 60)
    print("3. DATA QUALITY ANALYSIS")
    print("=" * 60)
    
    # Query 1: Data completeness check
    print("\nData Completeness Analysis:")
    print("-" * 40)
    
    completeness_query = """
        SELECT 
            'Temperature' as metric,
            COUNT(*) as total_records,
            SUM(CASE WHEN avg_temperature IS NOT NULL THEN 1 ELSE 0 END) as complete_records,
            ROUND(SUM(CASE WHEN avg_temperature IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as completeness_percentage
        FROM weather_data
        
        UNION ALL
        
        SELECT 
            'Precipitation' as metric,
            COUNT(*) as total_records,
            SUM(CASE WHEN total_precipitation IS NOT NULL THEN 1 ELSE 0 END) as complete_records,
            ROUND(SUM(CASE WHEN total_precipitation IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as completeness_percentage
        FROM weather_data
        
        UNION ALL
        
        SELECT 
            'Wind Speed' as metric,
            COUNT(*) as total_records,
            SUM(CASE WHEN avg_wind_speed IS NOT NULL THEN 1 ELSE 0 END) as complete_records,
            ROUND(SUM(CASE WHEN avg_wind_speed IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as completeness_percentage
        FROM weather_data
    """
    
    spark.sql(completeness_query).show()
    
    # Query 2: Data range validation
    print("\nData Range Validation:")
    print("-" * 40)
    
    range_validation_query = """
        SELECT 
            'Temperature' as metric,
            ROUND(MIN(avg_temperature), 1) as min_value,
            ROUND(MAX(avg_temperature), 1) as max_value,
            ROUND(AVG(avg_temperature), 1) as avg_value,
            SUM(CASE WHEN avg_temperature < -50 OR avg_temperature > 60 THEN 1 ELSE 0 END) as outliers
        FROM weather_data
        
        UNION ALL
        
        SELECT 
            'Precipitation' as metric,
            ROUND(MIN(total_precipitation), 1) as min_value,
            ROUND(MAX(total_precipitation), 1) as max_value,
            ROUND(AVG(total_precipitation), 1) as avg_value,
            SUM(CASE WHEN total_precipitation < 0 OR total_precipitation > 200 THEN 1 ELSE 0 END) as outliers
        FROM weather_data
        
        UNION ALL
        
        SELECT 
            'Wind Speed' as metric,
            ROUND(MIN(avg_wind_speed), 1) as min_value,
            ROUND(MAX(avg_wind_speed), 1) as max_value,
            ROUND(AVG(avg_wind_speed), 1) as avg_value,
            SUM(CASE WHEN avg_wind_speed < 0 OR avg_wind_speed > 200 THEN 1 ELSE 0 END) as outliers
        FROM weather_data
    """
    
    spark.sql(range_validation_query).show()
    
    # Query 3: Data consistency checks
    print("\nData Consistency Checks:")
    print("-" * 40)
    
    consistency_query = """
        SELECT 
            date,
            station,
            avg_tmax,
            avg_tmin,
            avg_temperature,
            CASE 
                WHEN avg_tmax < avg_tmin THEN 'ERROR: Max < Min'
                WHEN ABS(avg_temperature - (avg_tmax + avg_tmin)/2) > 1 THEN 'WARNING: Avg not between min/max'
                ELSE 'OK'
            END as consistency_check
        FROM weather_data
        WHERE avg_tmax < avg_tmin 
           OR ABS(avg_temperature - (avg_tmax + avg_tmin)/2) > 1
        ORDER BY date
    """
    
    result = spark.sql(consistency_query)
    if result.count() > 0:
        result.show()
    else:
        print("All temperature data is consistent!")


def demonstrate_business_intelligence_queries(spark: SparkSession, df):
    """Demonstrate business intelligence style queries."""
    print("=" * 60)
    print("4. BUSINESS INTELLIGENCE QUERIES")
    print("=" * 60)
    
    # Query 1: Monthly performance dashboard
    print("\nMonthly Weather Dashboard:")
    print("-" * 40)
    
    dashboard_query = """
        SELECT 
            month,
            CASE 
                WHEN month = 1 THEN 'January'
                WHEN month = 2 THEN 'February'
                WHEN month = 3 THEN 'March'
                ELSE 'Other'
            END as month_name,
            COUNT(*) as total_days,
            ROUND(AVG(avg_temperature), 1) as avg_temperature,
            ROUND(MIN(avg_temperature), 1) as min_temperature,
            ROUND(MAX(avg_temperature), 1) as max_temperature,
            ROUND(SUM(total_precipitation), 1) as total_precipitation,
            SUM(CASE WHEN total_precipitation > 0 THEN 1 ELSE 0 END) as rainy_days,
            SUM(CASE WHEN avg_temperature < 0 THEN 1 ELSE 0 END) as freezing_days,
            SUM(CASE WHEN avg_temperature >= 25 THEN 1 ELSE 0 END) as hot_days,
            ROUND(AVG(avg_wind_speed), 1) as avg_wind_speed
        FROM weather_data
        GROUP BY month
        ORDER BY month
    """
    
    spark.sql(dashboard_query).show()
    
    # Query 2: Station performance comparison
    print("\nStation Performance Comparison:")
    print("-" * 40)
    
    station_performance_query = """
        WITH station_metrics AS (
            SELECT 
                station,
                COUNT(*) as data_points,
                ROUND(AVG(avg_temperature), 1) as avg_temp,
                ROUND(STDDEV(avg_temperature), 1) as temp_variability,
                ROUND(SUM(total_precipitation), 1) as total_precip,
                ROUND(AVG(avg_wind_speed), 1) as avg_wind
            FROM weather_data
            GROUP BY station
        )
        SELECT 
            station,
            data_points,
            avg_temp,
            temp_variability,
            total_precip,
            avg_wind,
            CASE 
                WHEN avg_temp > 15 THEN 'Warm Climate'
                WHEN avg_temp > 5 THEN 'Moderate Climate'
                ELSE 'Cold Climate'
            END as climate_category
        FROM station_metrics
        ORDER BY avg_temp DESC
    """
    
    spark.sql(station_performance_query).show()
    
    # Query 3: Seasonal trends analysis
    print("\nSeasonal Trends Analysis:")
    print("-" * 40)
    
    trends_query = """
        SELECT 
            month,
            CASE 
                WHEN month IN (12, 1, 2) THEN 'Winter'
                WHEN month IN (3, 4, 5) THEN 'Spring'
                WHEN month IN (6, 7, 8) THEN 'Summer'
                ELSE 'Fall'
            END as season,
            COUNT(*) as days,
            ROUND(AVG(avg_temperature), 1) as avg_temp,
            ROUND(SUM(total_precipitation), 1) as total_precip,
            ROUND(AVG(avg_wind_speed), 1) as avg_wind,
            ROUND(MAX(avg_temperature) - MIN(avg_temperature), 1) as temp_range
        FROM weather_data
        GROUP BY month
        ORDER BY month
    """
    
    spark.sql(trends_query).show()


def main():
    """Main function to run all Spark SQL examples."""
    print("Weather Data Analytics with Spark SQL")
    print("=" * 60)
    print("This example demonstrates various Spark SQL capabilities")
    print("for weather data analysis and business intelligence.")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session("WeatherAnalyticsExample")
    spark.sparkContext.setLogLevel("ERROR")  # Reduce log noise
    
    try:
        # Create sample data
        print("\nCreating sample weather data...")
        df = create_sample_weather_data(spark)
        
        print(f"Created dataset with {df.count()} records")
        print(f"Weather stations: {df.select('station').distinct().count()}")
        print(f"Date range: {df.select('date').agg({'date': 'min'}).collect()[0][0]} to {df.select('date').agg({'date': 'max'}).collect()[0][0]}")
        
        # Run all demonstrations
        demonstrate_basic_sql_queries(spark, df)
        demonstrate_advanced_sql_analytics(spark, df)
        demonstrate_data_quality_analysis(spark, df)
        demonstrate_business_intelligence_queries(spark, df)
        
        print("\n" + "=" * 60)
        print("All Spark SQL examples completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error running examples: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 