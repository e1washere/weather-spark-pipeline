#!/usr/bin/env python3
"""
NOAA Weather Data Ingestion Module

Downloads weather data from NOAA's public API and saves to landing directory.
"""

import logging
import os
import requests
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
from config import config


def setup_logging() -> None:
    """Configure logging for the ingestion process."""
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=config.LOG_FORMAT
    )


def download_noaa_data(
    station_id: str = "GHCND:USW00014735",  # JFK Airport weather station
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_dir: Optional[str] = None
) -> str:
    """
    Download weather data from NOAA's Climate Data Online API.
    
    Args:
        station_id: NOAA weather station identifier
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format  
        output_dir: Directory to save the downloaded CSV
        
    Returns:
        Path to the downloaded CSV file
        
    Raises:
        ValueError: If date format is invalid or dates are invalid
        OSError: If directory cannot be created or file cannot be written
        requests.RequestException: If download fails (for future API integration)
    """
    logger = logging.getLogger(__name__)
    
    if output_dir is None:
        output_dir = str(config.LANDING_DIR)
    
    # Default to last 30 days if no dates provided
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    # Validate date format
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        raise ValueError("Dates must be in YYYY-MM-DD format")
    
    # Create output directory
    output_path = Path(output_dir)
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Cannot create output directory {output_dir}: {e}")
        raise OSError(f"Failed to create output directory {output_dir}: {e}")
    
    # For demo purposes, we'll use a sample weather CSV format
    # In production, you'd use NOAA's actual API with authentication
    sample_data = generate_sample_weather_data(start_date, end_date)
    
    filename = f"weather_data_{station_id.replace(':', '_')}_{start_date}_{end_date}.csv"
    filepath = output_path / filename
    
    logger.info(f"Generating sample weather data for station {station_id}")
    logger.info(f"Date range: {start_date} to {end_date}")
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(sample_data)
        
        logger.info(f"Successfully saved weather data to {filepath}")
        return str(filepath)
        
    except OSError as e:
        logger.error(f"Cannot write data to {filepath}: {e}")
        raise OSError(f"Failed to write weather data to {filepath}: {e}")


def generate_sample_weather_data(start_date: str, end_date: str) -> str:
    """
    Generate sample weather data in CSV format for demo purposes.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        CSV string with sample weather data
        
    Raises:
        ValueError: If date format is invalid or date range is invalid
    """
    import random
    from datetime import datetime, timedelta
    
    try:
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}")
    
    if current_date > end_dt:
        raise ValueError("Start date must be before end date")
    
    csv_lines = [
        "DATE,STATION,TMAX,TMIN,PRCP,SNOW,SNWD,AWND"
    ]
    
    while current_date <= end_dt:
        # Generate realistic weather data
        tmax = random.randint(200, 350)  # Max temp in tenths of degrees C
        tmin = random.randint(100, tmax - 50)  # Min temp
        prcp = random.randint(0, 50) if random.random() < 0.3 else 0  # Precipitation
        snow = random.randint(0, 20) if random.random() < 0.1 else 0  # Snow
        snwd = random.randint(0, 100) if snow > 0 else 0  # Snow depth
        awnd = random.randint(10, 200)  # Wind speed
        
        # Some missing values for testing
        if random.random() < 0.05:
            tmax = ""
        if random.random() < 0.05:
            tmin = ""
        if random.random() < 0.02:
            prcp = ""
            
        csv_lines.append(
            f"{current_date.strftime('%Y-%m-%d')},USW00014735,{tmax},{tmin},{prcp},{snow},{snwd},{awnd}"
        )
        
        current_date += timedelta(days=1)
    
    return '\n'.join(csv_lines)


def main() -> None:
    """Main function to run the ingestion process."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting NOAA weather data ingestion")
        filepath = download_noaa_data()
        logger.info(f"Data ingestion completed successfully: {filepath}")
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        raise


if __name__ == "__main__":
    main() 