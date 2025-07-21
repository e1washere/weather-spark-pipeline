#!/usr/bin/env python3
"""
Data Profiling Script for Processed Weather Data
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min as spark_min, max as spark_max, isnan, isnull

def profile_parquet(parquet_path: str, report_path: str = "profile_report.md") -> None:
    spark = SparkSession.builder.appName("DataProfiler").getOrCreate()
    df = spark.read.parquet(parquet_path)
    row_count = df.count()
    columns = df.columns
    with open(report_path, "w") as f:
        f.write(f"# Data Profile Report\n\n")
        f.write(f"**Total Rows:** {row_count}\n\n")
        for col_name in columns:
            nulls = df.filter(isnull(col(col_name)) | isnan(col(col_name))).count()
            min_val = df.select(spark_min(col(col_name))).collect()[0][0]
            max_val = df.select(spark_max(col(col_name))).collect()[0][0]
            f.write(f"- **{col_name}**: nulls={nulls}, min={min_val}, max={max_val}\n")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python profile_data.py <parquet_path> [report_path]")
        sys.exit(1)
    parquet_path = sys.argv[1]
    report_path = sys.argv[2] if len(sys.argv) > 2 else "profile_report.md"
    profile_parquet(parquet_path, report_path) 