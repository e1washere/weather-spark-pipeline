# Weather Spark Pipeline

A professional ETL pipeline for processing NOAA weather data using Apache Spark and Python.

## Features

- **Data Ingestion**: Downloads weather data from NOAA stations
- **Data Transformation**: Cleans and processes data using PySpark
- **Data Storage**: Saves processed data in partitioned Parquet format
- **SQL Analytics**: Exposes data through Spark SQL views
- **Testing**: Comprehensive unit tests with pytest
- **Monitoring**: Structured logging throughout the pipeline

## Architecture

```
Raw Data (CSV) → Ingestion → Transformation → Processed Data (Parquet) → SQL Views
```

## Project Structure

```
weather-spark-pipeline/
├── data/
│   ├── landing/          # Raw CSV files
│   ├── processed/        # Processed Parquet files
│   └── output/           # Final output files
├── tests/
│   ├── test_ingest.py    # Ingestion tests
│   ├── test_transform.py # Transformation tests
│   └── __init__.py
├── ingest.py             # Data ingestion module
├── transform.py          # Data transformation module
├── main.py               # Main orchestration script
├── requirements.txt      # Python dependencies
├── pytest.ini          # Test configuration
├── run_pipeline.sh      # Pipeline runner script
└── README.md            # This file
```

## Quick Start

### Prerequisites

- Python 3.11+
- Apache Spark 3.5+
- Java 8 or 11

### Installation

1. Clone the repository:
```bash
git clone https://github.com/e1washere/weather-spark-pipeline.git
cd weather-spark-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the pipeline:
```bash
python main.py --start-date 2024-01-01 --end-date 2024-01-31
```

## Usage

### Command Line Options

```bash
python main.py [OPTIONS]

Options:
  --mode {ingest,transform,full}  Pipeline mode (default: full)
  --station-id TEXT               NOAA station ID (default: GHCND:USW00014735)
  --start-date TEXT               Start date YYYY-MM-DD
  --end-date TEXT                 End date YYYY-MM-DD
  --input-path TEXT               Input CSV file path
  --landing-dir TEXT              Raw data directory
  --output-dir TEXT               Processed data directory
  --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
```

### Examples

Run full pipeline:
```bash
python main.py --station-id GHCND:USW00014735 --start-date 2024-01-01 --end-date 2024-01-31
```

Run only ingestion:
```bash
python main.py --mode ingest --station-id GHCND:USW00014735
```

Run only transformation:
```bash
python main.py --mode transform --input-path data/landing/weather_data.csv
```

### Spark Submit

Run with spark-submit for better performance:

```bash
# Basic local run
spark-submit --master local[*] main.py

# With configuration
spark-submit --master local[*] \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  main.py --station-id GHCND:USW00014735

# With resource allocation
spark-submit --master local[*] \
  --driver-memory 4g \
  --executor-memory 2g \
  main.py --start-date 2024-01-01 --end-date 2024-12-31
```

## Pipeline Runner Script

Use the provided shell script for common operations:

```bash
# Check Spark installation
./run_pipeline.sh check-spark

# Install dependencies
./run_pipeline.sh install-deps

# Run tests
./run_pipeline.sh test

# Run full pipeline
./run_pipeline.sh run-full

# Run with spark-submit
./run_pipeline.sh run-spark

# Clean up temporary files
./run_pipeline.sh cleanup
```

## Data Processing

### Ingestion (`ingest.py`)

- Downloads weather data from NOAA stations
- Generates sample data for demonstration
- Saves CSV files to `data/landing/`

### Transformation (`transform.py`)

- Loads CSV data into Spark DataFrame
- Cleans missing values and data types
- Converts temperatures from tenths of degrees to Celsius
- Computes daily averages and totals
- Saves partitioned Parquet files by year/month
- Creates SQL views for analytics

### Data Schema

**Raw Data Fields:**
- `DATE`: Date in YYYY-MM-DD format
- `STATION`: Weather station identifier
- `TMAX`: Maximum temperature (tenths of degrees C)
- `TMIN`: Minimum temperature (tenths of degrees C)
- `PRCP`: Precipitation (tenths of mm)
- `SNOW`: Snow fall (mm)
- `SNWD`: Snow depth (mm)
- `AWND`: Average wind speed (tenths of m/s)

**Processed Data Fields:**
- `date`: Date (Date type)
- `station`: Station ID
- `year`, `month`, `day`: Date components
- `avg_tmax`, `avg_tmin`, `avg_temperature`: Daily averages (°C)
- `total_precipitation`: Daily total precipitation
- `total_snow`: Daily total snowfall
- `avg_wind_speed`: Daily average wind speed

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_ingest.py -v

# Run with coverage
python -m pytest tests/ --cov=. --cov-report=html
```

## Development

### Code Style

- Follows PEP-8 standards
- Uses type hints throughout
- Comprehensive docstrings
- Structured logging

### Adding New Features

1. Create feature branch
2. Add functionality with tests
3. Update documentation
4. Submit pull request

## Performance

The pipeline is optimized for:
- Adaptive query execution
- Partition pruning
- Columnar storage (Parquet)
- Configurable parallelism

## Monitoring

Logs are written to:
- Console (stdout)
- `pipeline.log` file

Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

## Deployment

### Local Development
```bash
python main.py
```

### Production with Spark Cluster
```bash
spark-submit --master spark://cluster:7077 \
  --executor-memory 4g \
  --total-executor-cores 8 \
  main.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Contact

For questions or issues, please open a GitHub issue. 