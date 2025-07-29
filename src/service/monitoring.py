#!/usr/bin/env python3
"""
Performance Monitoring Module

Tracks processing metrics and performance statistics for the ETL pipeline.
"""

import logging
import time
import psutil
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from ..config import config


class PerformanceMonitor:
    """Monitor and track ETL pipeline performance metrics."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.start_time = None
        self.end_time = None
        self.metrics = {}
        self.initial_memory = None
        
    def start_monitoring(self) -> None:
        """Start performance monitoring."""
        self.start_time = time.time()
        self.initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        self.logger.info("Performance monitoring started")
        
    def end_monitoring(self) -> None:
        """End performance monitoring and calculate metrics."""
        if self.start_time is None:
            self.logger.warning("Monitoring was not started")
            return
            
        self.end_time = time.time()
        self._calculate_metrics()
        self._log_metrics()
        
    def _calculate_metrics(self) -> None:
        """Calculate performance metrics."""
        processing_time = self.end_time - self.start_time
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        memory_used = final_memory - self.initial_memory
        
        self.metrics = {
            "processing_time_seconds": processing_time,
            "memory_used_mb": memory_used,
            "peak_memory_mb": final_memory,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "end_time": datetime.fromtimestamp(self.end_time).isoformat()
        }
        
    def _log_metrics(self) -> None:
        """Log performance metrics."""
        processing_time = self.metrics["processing_time_seconds"]
        memory_used = self.metrics["memory_used_mb"]
        
        self.logger.info(f"Processing completed in {processing_time:.2f} seconds")
        self.logger.info(f"Memory used: {memory_used:.2f} MB")
        
        # Check performance thresholds
        max_time = config.PERFORMANCE_THRESHOLDS["max_processing_time_seconds"]
        max_memory = config.PERFORMANCE_THRESHOLDS["max_memory_usage_gb"] * 1024  # Convert to MB
        
        if processing_time > max_time:
            self.logger.warning(f"Processing time {processing_time:.2f}s exceeds threshold {max_time}s")
            
        if memory_used > max_memory:
            self.logger.warning(f"Memory usage {memory_used:.2f}MB exceeds threshold {max_memory}MB")
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        return self.metrics.copy()


def log_dataframe_stats(df, stage: str) -> None:
    """
    Log DataFrame statistics for monitoring.
    
    Args:
        df: Spark DataFrame to analyze
        stage: Processing stage name
    """
    logger = logging.getLogger(__name__)
    
    try:
        row_count = df.count()
        column_count = len(df.columns)
        
        logger.info(f"[{stage}] DataFrame stats - Rows: {row_count}, Columns: {column_count}")
        
        # Log null counts for key columns
        for col_name in config.VALIDATION_RULES["required_columns"]:
            if col_name in df.columns:
                null_count = df.filter(df[col_name].isNull() | (df[col_name] == "")).count()
                null_percentage = (null_count / row_count * 100) if row_count > 0 else 0
                logger.info(f"[{stage}] {col_name} - Nulls: {null_count} ({null_percentage:.1f}%)")
                
    except Exception as e:
        logger.warning(f"Could not log DataFrame stats for {stage}: {e}")


def create_performance_report(monitor: PerformanceMonitor, validation_results: Dict[str, Any]) -> str:
    """
    Create a comprehensive performance report.
    
    Args:
        monitor: PerformanceMonitor instance
        validation_results: Data validation results
        
    Returns:
        Formatted performance report
    """
    metrics = monitor.get_metrics()
    
    report = f"""
# Performance Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Processing Metrics
- **Processing Time**: {metrics.get('processing_time_seconds', 0):.2f} seconds
- **Memory Used**: {metrics.get('memory_used_mb', 0):.2f} MB
- **Peak Memory**: {metrics.get('peak_memory_mb', 0):.2f} MB

## Data Quality Metrics
- **Overall Valid**: {validation_results.get('overall_valid', False)}
- **Schema Valid**: {validation_results.get('schema_valid', False)}
- **Ranges Valid**: {validation_results.get('ranges_valid', False)}
- **Dates Valid**: {validation_results.get('dates_valid', False)}
- **Quality Score**: {validation_results.get('quality_score', 0):.3f}

## Performance Thresholds
- **Max Processing Time**: {config.PERFORMANCE_THRESHOLDS['max_processing_time_seconds']}s
- **Min Quality Score**: {config.PERFORMANCE_THRESHOLDS['min_data_quality_score']}
- **Max Memory Usage**: {config.PERFORMANCE_THRESHOLDS['max_memory_usage_gb']}GB

## Issues Found
"""
    
    issues = validation_results.get('issues', [])
    if issues:
        for issue in issues:
            report += f"- {issue}\n"
    else:
        report += "- No issues found\n"
        
    return report


def save_performance_report(report: str, output_path: Optional[str] = None) -> None:
    """
    Save performance report to file.
    
    Args:
        report: Performance report content
        output_path: Path to save the report
    """
    if output_path is None:
        output_path = config.LOGS_DIR / f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)
        logging.getLogger(__name__).info(f"Performance report saved to {output_path}")
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to save performance report: {e}")


# Global monitor instance
performance_monitor = PerformanceMonitor() 