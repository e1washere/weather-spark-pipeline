#!/usr/bin/env python3
"""
Tests for performance monitoring module.
"""

import pytest
import time
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.service.monitoring import (
    PerformanceMonitor,
    log_dataframe_stats,
    create_performance_report,
    save_performance_report
)


@pytest.fixture(scope="function")
def spark_session():
    """Create SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("MonitoringTest") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create sample DataFrame for testing."""
    data = [
        ("2024-01-01", "GHCND:USW00014735", "250", "150", "0", "0", "0", "50"),
        ("2024-01-02", "GHCND:USW00014735", "300", "200", "50", "0", "0", "60"),
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


class TestPerformanceMonitor:
    """Test PerformanceMonitor class."""
    
    @pytest.mark.unit
    def test_performance_monitor_initialization(self):
        """Test PerformanceMonitor initialization."""
        monitor = PerformanceMonitor()
        
        assert monitor.start_time is None
        assert monitor.end_time is None
        assert monitor.metrics == {}
        assert monitor.initial_memory is None
    
    @pytest.mark.unit
    def test_start_monitoring(self):
        """Test starting performance monitoring."""
        monitor = PerformanceMonitor()
        
        with patch('psutil.Process') as mock_process:
            mock_process.return_value.memory_info.return_value.rss = 1024 * 1024  # 1MB
            monitor.start_monitoring()
        
        assert monitor.start_time is not None
        assert monitor.initial_memory == 1.0  # 1MB
    
    @pytest.mark.unit
    def test_end_monitoring_without_start(self):
        """Test ending monitoring without starting it."""
        monitor = PerformanceMonitor()
        
        with patch('psutil.Process') as mock_process:
            mock_process.return_value.memory_info.return_value.rss = 2048 * 1024  # 2MB
            monitor.end_monitoring()
        
        assert monitor.end_time is None
        assert monitor.metrics == {}
    
    @pytest.mark.unit
    def test_end_monitoring_with_start(self):
        """Test ending monitoring after starting it."""
        monitor = PerformanceMonitor()
        
        with patch('psutil.Process') as mock_process:
            mock_process.return_value.memory_info.return_value.rss = 1024 * 1024  # 1MB
            monitor.start_monitoring()
            
            time.sleep(0.1)  # Small delay to ensure different timestamps
            
            mock_process.return_value.memory_info.return_value.rss = 2048 * 1024  # 2MB
            monitor.end_monitoring()
        
        assert monitor.end_time is not None
        assert "processing_time_seconds" in monitor.metrics
        assert "memory_used_mb" in monitor.metrics
        assert "peak_memory_mb" in monitor.metrics
        assert monitor.metrics["memory_used_mb"] == 1.0  # 2MB - 1MB
    
    @pytest.mark.unit
    def test_get_metrics(self):
        """Test getting metrics."""
        monitor = PerformanceMonitor()
        monitor.metrics = {"test": "value"}
        
        metrics = monitor.get_metrics()
        
        assert metrics == {"test": "value"}
        assert metrics is not monitor.metrics  # Should be a copy
    
    @pytest.mark.unit
    def test_performance_threshold_warnings(self):
        """Test performance threshold warnings."""
        monitor = PerformanceMonitor()
        
        with patch('psutil.Process') as mock_process:
            mock_process.return_value.memory_info.return_value.rss = 1024 * 1024  # 1MB
            monitor.start_monitoring()
            
            # Simulate high memory usage
            mock_process.return_value.memory_info.return_value.rss = 5 * 1024 * 1024 * 1024  # 5GB
            monitor.end_monitoring()
        
        # Should have warnings for exceeding memory threshold
        assert "peak_memory_mb" in monitor.metrics


class TestMonitoringFunctions:
    """Test monitoring utility functions."""
    
    @pytest.mark.unit
    def test_log_dataframe_stats(self, sample_dataframe, caplog):
        """Test logging DataFrame statistics."""
        with caplog.at_level("INFO"):
            log_dataframe_stats(sample_dataframe, "TEST_STAGE")
        
        # Check that stats were logged
        assert "TEST_STAGE" in caplog.text
        assert "Rows: 3" in caplog.text
        assert "Columns: 8" in caplog.text
    
    @pytest.mark.unit
    def test_log_dataframe_stats_with_nulls(self, spark_session, caplog):
        """Test logging DataFrame statistics with null values."""
        data = [
            ("2024-01-01", "GHCND:USW00014735", "250", "150", "0", "0", "0", "50"),
            ("2024-01-02", "GHCND:USW00014735", None, None, "50", "0", "0", "60"),  # Nulls
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
        
        df = spark_session.createDataFrame(data, schema)
        
        with caplog.at_level("INFO"):
            log_dataframe_stats(df, "TEST_STAGE")
        
        # Check that null statistics were logged
        assert "TEST_STAGE" in caplog.text
        assert "Nulls:" in caplog.text
    
    @pytest.mark.unit
    def test_log_dataframe_stats_exception(self, spark_session, caplog):
        """Test logging DataFrame statistics with exception."""
        # Create DataFrame that will cause an error
        data = [("2024-01-01", "GHCND:USW00014735", "250", "150")]
        schema = StructType([
            StructField("DATE", StringType(), True),
            StructField("STATION", StringType(), True),
            StructField("TMAX", StringType(), True),
            StructField("TMIN", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Mock count() to raise exception
        with patch.object(df, 'count', side_effect=Exception("Test error")):
            with caplog.at_level("WARNING"):
                log_dataframe_stats(df, "TEST_STAGE")
        
        assert "Could not log DataFrame stats" in caplog.text
    
    @pytest.mark.unit
    def test_create_performance_report(self):
        """Test creating performance report."""
        monitor = PerformanceMonitor()
        monitor.metrics = {
            "processing_time_seconds": 10.5,
            "memory_used_mb": 512.0,
            "peak_memory_mb": 1024.0
        }
        
        validation_results = {
            "overall_valid": True,
            "schema_valid": True,
            "ranges_valid": True,
            "dates_valid": True,
            "quality_score": 0.95,
            "issues": []
        }
        
        report = create_performance_report(monitor, validation_results)
        
        assert "Performance Report" in report
        assert "10.50" in report  # Processing time
        assert "512.00" in report  # Memory used
        assert "0.950" in report  # Quality score
        assert "No issues found" in report
    
    @pytest.mark.unit
    def test_create_performance_report_with_issues(self):
        """Test creating performance report with issues."""
        monitor = PerformanceMonitor()
        monitor.metrics = {
            "processing_time_seconds": 5.0,
            "memory_used_mb": 256.0,
            "peak_memory_mb": 512.0
        }
        
        validation_results = {
            "overall_valid": False,
            "schema_valid": True,
            "ranges_valid": False,
            "dates_valid": True,
            "quality_score": 0.75,
            "issues": ["Temperature out of range", "Invalid date format"]
        }
        
        report = create_performance_report(monitor, validation_results)
        
        assert "Performance Report" in report
        assert "Temperature out of range" in report
        assert "Invalid date format" in report
    
    @pytest.mark.unit
    def test_save_performance_report(self, tmp_path):
        """Test saving performance report to file."""
        monitor = PerformanceMonitor()
        monitor.metrics = {
            "processing_time_seconds": 5.0,
            "memory_used_mb": 256.0,
            "peak_memory_mb": 512.0
        }
        
        validation_results = {
            "overall_valid": True,
            "schema_valid": True,
            "ranges_valid": True,
            "dates_valid": True,
            "quality_score": 0.95,
            "issues": []
        }
        
        report = create_performance_report(monitor, validation_results)
        output_path = tmp_path / "test_report.md"
        
        save_performance_report(report, str(output_path))
        
        assert output_path.exists()
        with open(output_path, 'r') as f:
            content = f.read()
            assert "Performance Report" in content
    
    @pytest.mark.unit
    def test_save_performance_report_default_path(self, tmp_path):
        """Test saving performance report with default path."""
        with patch('src.service.monitoring.config') as mock_config:
            mock_config.LOGS_DIR = tmp_path
            
            monitor = PerformanceMonitor()
            monitor.metrics = {"processing_time_seconds": 5.0}
            
            validation_results = {
                "overall_valid": True,
                "schema_valid": True,
                "ranges_valid": True,
                "dates_valid": True,
                "quality_score": 1.0,
                "issues": []
            }
            
            report = create_performance_report(monitor, validation_results)
            save_performance_report(report)
            
            # Check that a file was created with timestamp
            log_files = list(tmp_path.glob("performance_report_*.md"))
            assert len(log_files) == 1
    
    @pytest.mark.unit
    def test_save_performance_report_exception(self, tmp_path):
        """Test saving performance report with exception."""
        report = "Test report content"
        # Create a path that will cause an exception (file in a directory that doesn't exist)
        invalid_path = tmp_path / "nonexistent" / "report.md"
        
        with patch('src.service.monitoring.logging.getLogger') as mock_logger:
            mock_logger.return_value.error = MagicMock()
            
            # Mock Path.mkdir to raise an exception
            with patch('pathlib.Path.mkdir', side_effect=OSError("Permission denied")):
                save_performance_report(report, str(invalid_path))
            
            # Should log error but not raise exception
            mock_logger.return_value.error.assert_called_once() 