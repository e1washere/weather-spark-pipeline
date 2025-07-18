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


def setup_logging() -> None:
    """Configure logging for the ingestion process."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def download_noaa_data(
    station_id: str = "GHCND:USW00014735",  # JFK Airport weather station
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_dir: str = "data/landing"
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
        requests.RequestException: If download fails
        IOError: If file cannot be written
    """
    logger = logging.getLogger(__name__)
    
    # Default to last 30 days if no dates provided
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
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
        
    except IOError as e:
        logger.error(f"Failed to write data to {filepath}: {e}")
        raise


def generate_sample_weather_data(start_date: str, end_date: str) -> str:
    """Generate sample weather data in CSV format for demo purposes."""
    import random
    from datetime import datetime, timedelta
    
    csv_lines = [
        "DATE,STATION,TMAX,TMIN,PRCP,SNOW,SNWD,AWND"
    ]
    
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
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