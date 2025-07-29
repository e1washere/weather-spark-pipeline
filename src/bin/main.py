#!/usr/bin/env python3
"""
Weather ETL Pipeline Main Script

Orchestrates the complete ETL pipeline for weather data processing.
"""

import argparse
import logging
import sys
import os
from pathlib import Path
from typing import Optional

import requests
from py4j.protocol import Py4JJavaError

from ..service import download_noaa_data, transform_weather_data
from ..config import config


def setup_logging() -> None:
    """Configure logging for the main application."""
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(config.LOGS_DIR / config.LOG_FILE)
        ]
    )


def run_ingestion(station_id: str, start_date: Optional[str] = None, 
                  end_date: Optional[str] = None) -> str:
    """
    Run data ingestion process.
    
    Args:
        station_id: Weather station identifier
        start_date: Start date for data range
        end_date: End date for data range
        
    Returns:
        Path to the downloaded data file
        
    Raises:
        ValueError: If parameters are invalid
        OSError: If file system operations fail
        requests.RequestException: If download fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Starting data ingestion for station {station_id}")
        
        filepath = download_noaa_data(
            station_id=station_id,
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info(f"Data ingestion completed: {filepath}")
        return filepath
        
    except ValueError as e:
        logger.error(f"Data ingestion failed - invalid parameters: {e}")
        raise ValueError(f"Ingestion failed due to invalid parameters: {e}")
    except OSError as e:
        logger.error(f"Data ingestion failed - file system error: {e}")
        raise OSError(f"Ingestion failed due to file system error: {e}")
    except requests.RequestException as e:
        logger.error(f"Data ingestion failed - network error: {e}")
        raise requests.RequestException(f"Ingestion failed due to network error: {e}")


def run_transformation(input_path: str, output_path: Optional[str] = None) -> None:
    """
    Run data transformation process.
    
    Args:
        input_path: Path to input data file
        output_path: Path for output data
        
    Raises:
        FileNotFoundError: If input file not found
        Py4JJavaError: If Spark processing fails
        OSError: If file system operations fail
        ValueError: If data validation fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Starting data transformation from {input_path}")
        
        transform_weather_data(input_path=input_path, output_path=output_path)
        
        logger.info("Data transformation completed successfully")
        
    except FileNotFoundError as e:
        logger.error(f"Data transformation failed - file not found: {e}")
        raise FileNotFoundError(f"Transformation failed - input file not found: {e}")
    except Py4JJavaError as e:
        logger.error(f"Data transformation failed - Spark error: {e}")
        raise Py4JJavaError(f"Transformation failed - Spark error: {e}")
    except OSError as e:
        logger.error(f"Data transformation failed - file system error: {e}")
        raise OSError(f"Transformation failed - file system error: {e}")
    except ValueError as e:
        logger.error(f"Data transformation failed - invalid data: {e}")
        raise ValueError(f"Transformation failed - invalid data: {e}")


def run_full_pipeline(station_id: str, start_date: Optional[str] = None, 
                     end_date: Optional[str] = None, output_path: Optional[str] = None) -> None:
    """
    Run complete ETL pipeline.
    
    Args:
        station_id: Weather station identifier
        start_date: Start date for data range
        end_date: End date for data range
        output_path: Path for output data
        
    Raises:
        FileNotFoundError: If input file not found
        Py4JJavaError: If Spark processing fails
        OSError: If file system operations fail
        ValueError: If data validation fails
        requests.RequestException: If download fails
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting full ETL pipeline")
        
        # Step 1: Data ingestion
        input_file = run_ingestion(station_id, start_date, end_date)
        
        # Step 2: Data transformation
        run_transformation(input_file, output_path)
        
        logger.info("Full ETL pipeline completed successfully")
        
    except (FileNotFoundError, Py4JJavaError, OSError, ValueError, requests.RequestException) as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise


def validate_input_parameters(args: argparse.Namespace) -> None:
    """
    Validate command line parameters.
    
    Args:
        args: Parsed command line arguments
        
    Raises:
        ValueError: If parameters are invalid
    """
    if args.mode == "ingest" and not args.station_id:
        raise ValueError("--station-id is required for ingest mode")
    
    if args.mode == "transform" and not args.input_path:
        raise ValueError("--input-path is required when using transform mode")
    
    if args.mode == "full" and not args.station_id:
        raise ValueError("--station-id is required for full pipeline mode")


def main() -> None:
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(
        description="Weather ETL Pipeline - Process NOAA weather data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode full --station-id GHCND:USW00014735
  %(prog)s --mode ingest --station-id GHCND:USW00014735 --start-date 2024-01-01 --end-date 2024-01-31
  %(prog)s --mode transform --input-path data/landing/weather_data_*.csv
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["ingest", "transform", "full"],
        default="full",
        help="Pipeline execution mode (default: full)"
    )
    
    parser.add_argument(
        "--station-id",
        default="GHCND:USW00014735",
        help="NOAA weather station identifier (default: GHCND:USW00014735)"
    )
    
    parser.add_argument(
        "--start-date",
        help="Start date for data range (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date", 
        help="End date for data range (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--input-path",
        help="Path to input CSV file(s) for transform mode"
    )
    
    parser.add_argument(
        "--output-path",
        help="Path for output Parquet files"
    )
    
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Run data validation only without processing"
    )
    
    parser.add_argument(
        "--performance-report",
        action="store_true",
        help="Generate detailed performance report"
    )
    
    args = parser.parse_args()
    
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Ensure directories exist
        config.ensure_directories()
        
        # Validate parameters
        validate_input_parameters(args)
        
        logger.info(f"Starting Weather ETL Pipeline in {args.mode} mode")
        
        # Execute based on mode
        if args.mode == "ingest":
            run_ingestion(args.station_id, args.start_date, args.end_date)
        elif args.mode == "transform":
            run_transformation(args.input_path, args.output_path)
        elif args.mode == "full":
            run_full_pipeline(args.station_id, args.start_date, args.end_date, args.output_path)
        
        logger.info("Pipeline execution completed successfully")
        
    except (ValueError, FileNotFoundError, OSError, requests.RequestException) as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 