#!/usr/bin/env python3
"""
Weather Data ETL Pipeline

Main orchestration script for the weather data ETL pipeline.
Coordinates ingestion and transformation processes.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional
import requests
from py4j.protocol import Py4JJavaError

from ..service import download_noaa_data, transform_weather_data
from ..config import config


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure logging for the entire pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level = getattr(logging, log_level.upper())
    logging.basicConfig(
        level=level,
        format=config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pipeline.log')
        ]
    )


def run_ingestion(
    station_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_dir: Optional[str] = None
) -> str:
    """
    Run the data ingestion process.
    
    Args:
        station_id: NOAA weather station identifier
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_dir: Directory to save downloaded data
        
    Returns:
        Path to the downloaded CSV file
        
    Raises:
        ValueError: If date format is invalid or station_id is invalid
        OSError: If output directory cannot be created
        requests.RequestException: If download fails
    """
    logger = logging.getLogger(__name__)
    
    if output_dir is None:
        output_dir = str(config.LANDING_DIR)
    
    logger.info("=" * 50)
    logger.info("STARTING DATA INGESTION")
    logger.info("=" * 50)
    
    try:
        filepath = download_noaa_data(
            station_id=station_id,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir
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


def run_transformation(
    input_path: Optional[str] = None,
    output_path: Optional[str] = None
) -> None:
    """
    Run the data transformation process.
    
    Args:
        input_path: Path to input CSV file
        output_path: Path to save processed Parquet files
        
    Raises:
        FileNotFoundError: If input file doesn't exist
        Py4JJavaError: If Spark operations fail
        OSError: If output directory cannot be created
        ValueError: If input data is invalid
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 50)
    logger.info("STARTING DATA TRANSFORMATION")
    logger.info("=" * 50)
    
    try:
        transform_weather_data(input_path, output_path)
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


def run_full_pipeline(
    station_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    landing_dir: Optional[str] = None,
    output_dir: Optional[str] = None
) -> None:
    """
    Run the complete ETL pipeline.
    
    Args:
        station_id: NOAA weather station identifier
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        landing_dir: Directory for raw data
        output_dir: Directory for processed data
        
    Raises:
        FileNotFoundError: If input file doesn't exist
        Py4JJavaError: If Spark operations fail
        OSError: If directories cannot be created
        ValueError: If parameters or data are invalid
        requests.RequestException: If download fails
    """
    logger = logging.getLogger(__name__)
    
    if landing_dir is None:
        landing_dir = str(config.LANDING_DIR)
    
    if output_dir is None:
        output_dir = config.get_output_path()
    
    logger.info("=" * 60)
    logger.info("STARTING WEATHER DATA ETL PIPELINE")
    logger.info("=" * 60)
    
    try:
        # Ensure directories exist
        config.ensure_directories()
        
        # Step 1: Data Ingestion
        csv_filepath = run_ingestion(
            station_id=station_id,
            start_date=start_date,
            end_date=end_date,
            output_dir=landing_dir
        )
        
        # Step 2: Data Transformation
        run_transformation(
            input_path=csv_filepath,
            output_path=output_dir
        )
        
        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Processed data saved to: {output_dir}")
        
    except (FileNotFoundError, Py4JJavaError, OSError, ValueError, requests.RequestException) as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Weather Data ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python main.py --station-id GHCND:USW00014735 --start-date 2024-01-01 --end-date 2024-01-31

  # Run only ingestion
  python main.py --mode ingest --station-id GHCND:USW00014735

  # Run only transformation
  python main.py --mode transform --input-path data/landing/weather_data.csv

  # Run with spark-submit
  spark-submit --master local[*] main.py --station-id GHCND:USW00014735
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["ingest", "transform", "full"],
        default="full",
        help="Pipeline mode to run (default: full)"
    )
    
    parser.add_argument(
        "--station-id",
        default="GHCND:USW00014735",
        help="NOAA weather station identifier (default: GHCND:USW00014735)"
    )
    
    parser.add_argument(
        "--start-date",
        help="Start date in YYYY-MM-DD format (default: 30 days ago)"
    )
    
    parser.add_argument(
        "--end-date",
        help="End date in YYYY-MM-DD format (default: today)"
    )
    
    parser.add_argument(
        "--input-path",
        help="Path to input CSV file (required for transform mode)"
    )
    
    parser.add_argument(
        "--landing-dir",
        help="Directory for raw data (default: from config)"
    )
    
    parser.add_argument(
        "--output-dir",
        help="Directory for processed data (default: from config)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """
    Validate command-line arguments.
    
    Args:
        args: Parsed command-line arguments
        
    Raises:
        ValueError: If arguments are invalid
    """
    if args.mode == "transform" and not args.input_path:
        raise ValueError("--input-path is required when using transform mode")
    
    if args.start_date and args.end_date:
        from datetime import datetime
        try:
            start = datetime.strptime(args.start_date, "%Y-%m-%d")
            end = datetime.strptime(args.end_date, "%Y-%m-%d")
            if start > end:
                raise ValueError("Start date must be before end date")
        except ValueError as e:
            if "time data" in str(e):
                raise ValueError("Dates must be in YYYY-MM-DD format")
            raise


def main() -> None:
    """Main function to run the ETL pipeline."""
    try:
        # Parse command-line arguments
        parser = create_parser()
        args = parser.parse_args()
        
        # Validate arguments
        validate_args(args)
        
        # Setup logging
        setup_logging(args.log_level)
        logger = logging.getLogger(__name__)
        
        # Ensure directories exist
        config.ensure_directories()
        
        # Run the appropriate pipeline mode
        if args.mode == "ingest":
            run_ingestion(
                station_id=args.station_id,
                start_date=args.start_date,
                end_date=args.end_date,
                output_dir=args.landing_dir
            )
        elif args.mode == "transform":
            run_transformation(
                input_path=args.input_path,
                output_path=args.output_dir
            )
        else:  # full pipeline
            run_full_pipeline(
                station_id=args.station_id,
                start_date=args.start_date,
                end_date=args.end_date,
                landing_dir=args.landing_dir,
                output_dir=args.output_dir
            )
            
    except (ValueError, FileNotFoundError, OSError, requests.RequestException) as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 