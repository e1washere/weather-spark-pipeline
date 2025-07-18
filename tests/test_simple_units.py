#!/usr/bin/env python3
"""
Simple focused unit tests for basic functionality.
"""

import pytest
import tempfile
import os
from datetime import datetime, timedelta

from ingest import generate_sample_weather_data, download_noaa_data


@pytest.mark.unit
class TestSimpleUnits:
    """Simple unit tests for basic functionality."""
    
    def test_sample_weather_data_generation(self):
        """Simple unit test: Generate sample weather data for a specific date range."""
        start_date = "2024-01-01"
        end_date = "2024-01-03"
        
        # Generate sample data
        result = generate_sample_weather_data(start_date, end_date)
        
        # Verify basic structure
        lines = result.strip().split('\n')
        
        # Should have header + 3 days of data
        assert len(lines) == 4
        
        # Check header format
        header = lines[0].split(',')
        expected_columns = ["DATE", "STATION", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        assert header == expected_columns
        
        # Check each data line has correct structure
        for i, line in enumerate(lines[1:], 1):
            fields = line.split(',')
            assert len(fields) == 8
            
            # Check date format
            date_field = fields[0]
            assert date_field == f"2024-01-0{i}"
            
            # Check station ID
            station_field = fields[1]
            assert station_field == "USW00014735"
            
            # Check that numeric fields are either empty or numeric
            for j in range(2, 8):
                field = fields[j]
                if field:  # Not empty
                    try:
                        int(field)  # Should be parseable as integer
                    except ValueError:
                        assert False, f"Field {j} should be numeric or empty: {field}"
    
    def test_date_range_validation(self):
        """Simple unit test: Validate date range calculation."""
        start_date = "2024-01-01"
        end_date = "2024-01-05"
        
        # Parse dates
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Calculate expected number of days
        expected_days = (end_dt - start_dt).days + 1
        assert expected_days == 5
        
        # Generate data and verify
        result = generate_sample_weather_data(start_date, end_date)
        lines = result.strip().split('\n')
        
        # Should have header + expected_days
        assert len(lines) == expected_days + 1
        
        # Verify date sequence
        for i, line in enumerate(lines[1:], 1):
            expected_date = (start_dt + timedelta(days=i-1)).strftime('%Y-%m-%d')
            actual_date = line.split(',')[0]
            assert actual_date == expected_date
    
    def test_file_creation_and_cleanup(self):
        """Simple unit test: Test file creation and cleanup."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test file creation
            result_path = download_noaa_data(
                station_id="GHCND:USW00014735",
                start_date="2024-01-01",
                end_date="2024-01-02",
                output_dir=temp_dir
            )
            
            # Verify file exists
            assert os.path.exists(result_path)
            
            # Verify file content
            with open(result_path, 'r') as f:
                content = f.read()
                assert "DATE,STATION,TMAX,TMIN,PRCP,SNOW,SNWD,AWND" in content
                assert "2024-01-01" in content
                assert "2024-01-02" in content
                assert "USW00014735" in content
            
            # Verify filename format
            filename = os.path.basename(result_path)
            assert "GHCND_USW00014735" in filename
            assert "2024-01-01_2024-01-02" in filename
            assert filename.endswith(".csv")
    
    def test_station_id_formatting(self):
        """Simple unit test: Test station ID formatting in filenames."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test with different station IDs
            test_cases = [
                ("GHCND:USW00014735", "GHCND_USW00014735"),
                ("GHCND:USW00012345", "GHCND_USW00012345"),
                ("GHCND:USC00123456", "GHCND_USC00123456"),
            ]
            
            for station_id, expected_format in test_cases:
                result_path = download_noaa_data(
                    station_id=station_id,
                    start_date="2024-01-01",
                    end_date="2024-01-01",
                    output_dir=temp_dir
                )
                
                filename = os.path.basename(result_path)
                assert expected_format in filename
                
                # Clean up
                os.remove(result_path)
    
    def test_weather_data_bounds(self):
        """Simple unit test: Test weather data stays within reasonable bounds."""
        start_date = "2024-01-01"
        end_date = "2024-01-10"
        
        result = generate_sample_weather_data(start_date, end_date)
        lines = result.strip().split('\n')
        
        # Check data bounds for each record
        for line in lines[1:]:  # Skip header
            fields = line.split(',')
            
            # Check temperature fields (TMAX, TMIN) - should be reasonable values
            tmax_field = fields[2]
            tmin_field = fields[3]
            
            if tmax_field:  # Not empty
                tmax = int(tmax_field)
                # Temperature in tenths of degrees Celsius: -50°C to 50°C
                assert -500 <= tmax <= 500
                
            if tmin_field:  # Not empty
                tmin = int(tmin_field)
                # Temperature in tenths of degrees Celsius: -50°C to 50°C
                assert -500 <= tmin <= 500
                
            # Check precipitation (PRCP) - should be non-negative
            prcp_field = fields[4]
            if prcp_field:  # Not empty
                prcp = int(prcp_field)
                assert prcp >= 0
                
            # Check wind speed (AWND) - should be non-negative
            awnd_field = fields[7]
            if awnd_field:  # Not empty
                awnd = int(awnd_field)
                assert awnd >= 0
    
    def test_empty_date_range(self):
        """Simple unit test: Test handling of same start and end date."""
        start_date = "2024-01-01"
        end_date = "2024-01-01"
        
        result = generate_sample_weather_data(start_date, end_date)
        lines = result.strip().split('\n')
        
        # Should have header + 1 day
        assert len(lines) == 2
        
        # Check the single data line
        data_line = lines[1]
        fields = data_line.split(',')
        
        assert fields[0] == "2024-01-01"
        assert fields[1] == "USW00014735"
        assert len(fields) == 8


if __name__ == "__main__":
    pytest.main([__file__]) 