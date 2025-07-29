# Weather ETL Pipeline

A scalable ETL pipeline using Apache Spark to process and analyze weather data from NOAA. This project demonstrates modern data engineering practices including data validation, performance monitoring, and comprehensive testing.

## Architecture

The pipeline follows the **Medallion Architecture** pattern:

- **Bronze Layer**: Raw weather data from NOAA API
- **Silver Layer**: Cleaned and validated data
- **Gold Layer**: Aggregated daily weather statistics

### Project Structure

```
src/
├── config/          # Configuration management
├── service/         # Core ETL services
│   ├── ingest.py    # Data ingestion from NOAA
│   ├── transform.py # PySpark data processing
│   ├── validation.py # Data quality validation
│   └── monitoring.py # Performance monitoring
└── bin/            # Command-line interface

tests/              # Comprehensive test suite
data/               # Data directories
├── landing/        # Raw data storage
├── processed/      # Processed data storage
└── logs/          # Performance reports
```

## Features

- **Data Ingestion**: Download weather data from NOAA API
- **Data Transformation**: Clean, validate, and aggregate using PySpark
- **Data Validation**: Comprehensive data quality checks
- **Performance Monitoring**: Real-time metrics and reporting
- **SQL Views**: Temporary views for data analysis
- **Comprehensive Testing**: 50+ unit and integration tests
- **Production Ready**: Error handling, logging, and configuration management

## Quick Start

### Prerequisites

- Python 3.11+
- Apache Spark 3.5.0
- 4GB+ RAM recommended

### Installation

```bash
# Clone repository
git clone https://github.com/e1washere/weather-spark-pipeline.git
cd weather-spark-pipeline

# Install dependencies
pip install -r requirements.txt

# Run tests to verify installation
make test
```

### Basic Usage

```bash
# Complete pipeline execution
python3 -m src.bin.main --mode full --station-id GHCND:USW00014735

# Individual components
python3 -m src.bin.main --mode ingest --start-date 2024-01-01 --end-date 2024-01-31
python3 -m src.bin.main --mode transform --input-path data/landing/weather_2024-01-01_2024-01-31.csv

# Using Spark Submit (recommended for production)
spark-submit --master local[*] src/bin/main.py --mode full
```

## Data Processing

### 1. Data Ingestion

Downloads weather data from NOAA's Climate Data Online API:

```python
from src.service import download_noaa_data

# Download weather data
filepath = download_noaa_data(
    station_id="GHCND:USW00014735",
    start_date="2024-01-01",
    end_date="2024-01-31"
)
```

### 2. Data Transformation

Processes raw data using PySpark:

```python
from src.service import transform_weather_data

# Transform weather data
transform_weather_data(
    input_path="data/landing/weather_data.csv",
    output_path="data/processed/weather_parquet"
)
```

### 3. Data Validation

Comprehensive data quality checks:

```python
from src.service import validate_weather_data

# Validate data quality
validation_results = validate_weather_data(df)
print(f"Data quality score: {validation_results['quality_score']:.3f}")
```

### 4. Performance Monitoring

Real-time performance tracking:

```python
from src.service import performance_monitor

# Start monitoring
performance_monitor.start_monitoring()

# ... processing ...

# End monitoring and get metrics
performance_monitor.end_monitoring()
metrics = performance_monitor.get_metrics()
```

## Data Quality

The pipeline includes comprehensive data validation:

- **Schema Validation**: Ensures required columns and data types
- **Range Validation**: Validates temperature, precipitation, and wind ranges
- **Date Validation**: Checks date format and consistency
- **Quality Scoring**: Calculates overall data quality score

### Validation Rules

```python
VALIDATION_RULES = {
    "temperature_range": {"min": -50, "max": 60},
    "precipitation_range": {"min": 0, "max": 1000},
    "snow_range": {"min": 0, "max": 500},
    "wind_range": {"min": 0, "max": 100},
    "required_columns": ["DATE", "STATION", "TMAX", "TMIN"],
    "date_format": "%Y-%m-%d"
}
```

## Performance Monitoring

The pipeline tracks key performance metrics:

- **Processing Time**: Total execution time
- **Memory Usage**: Peak and average memory consumption
- **Data Quality**: Validation scores and issues
- **Performance Thresholds**: Configurable alerts

### Performance Reports

Automatically generated performance reports include:

- Processing metrics and timing
- Data quality assessment
- Performance threshold analysis
- Issue identification and recommendations

## Testing

Comprehensive test suite with 50+ tests:

```bash
# Run all tests
make test

# Run specific test categories
pytest tests/ -m unit -v
pytest tests/ -m integration -v
pytest tests/ -m performance -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

### Test Categories

- **Unit Tests**: Individual function testing
- **Integration Tests**: End-to-end pipeline testing
- **Performance Tests**: Load and stress testing
- **Validation Tests**: Data quality validation testing

## Configuration

Centralized configuration management:

```python
from src.config import config

# Access configuration
spark_configs = config.SPARK_CONFIGS
validation_rules = config.VALIDATION_RULES
performance_thresholds = config.PERFORMANCE_THRESHOLDS
```

### Key Configuration Areas

- **Spark Settings**: Performance and optimization parameters
- **Data Validation**: Quality rules and thresholds
- **Performance Monitoring**: Metrics collection and alerts
- **File Paths**: Input/output directory configuration

## Development

### Code Quality

```bash
# Linting
make lint

# Type checking
make type-check

# Format code
make format
```

### Adding New Features

1. **Modular Design**: Each component has a single responsibility
2. **Dependency Injection**: Functions accept dependencies as parameters
3. **Comprehensive Testing**: All new features require tests
4. **Documentation**: Update docstrings and README

### Best Practices

- **Error Handling**: Use specific exception types
- **Logging**: Structured logging with appropriate levels
- **Configuration**: Externalize all configurable parameters
- **Testing**: Maintain high test coverage

## Production Deployment

### Spark Configuration

```bash
spark-submit \
    --master spark://cluster:7077 \
    --executor-memory 4g \
    --total-executor-cores 8 \
    --conf spark.sql.adaptive.enabled=true \
    src/bin/main.py --mode full
```

### Monitoring and Alerting

- Performance reports are automatically generated
- Data quality scores are tracked over time
- Threshold violations trigger alerts
- Logs are structured for easy parsing

## Troubleshooting

### Common Issues

1. **Memory Issues**: Increase executor memory or reduce partition size
2. **Performance Issues**: Enable adaptive query execution
3. **Data Quality Issues**: Check validation rules and data sources
4. **Test Failures**: Ensure Spark is properly configured

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python3 -m src.bin.main --mode full
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- NOAA Climate Data Online for weather data
- Apache Spark community for the processing framework
- PySpark developers for Python integration 
 