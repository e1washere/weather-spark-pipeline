# Weather Spark ETL Pipeline

A professional-grade ETL pipeline for processing NOAA weather data using Apache Spark and Python. This project demonstrates modern data engineering practices with comprehensive testing, monitoring, and deployment automation.

## Project Goals

- **Scalable Data Processing**: Handle large volumes of weather data using Apache Spark
- **Professional Development**: Demonstrate enterprise-level coding practices and testing
- **Real-World Data Pipeline**: Process actual NOAA weather station data
- **Modern Architecture**: Implement clean, maintainable ETL patterns
- **Comprehensive Testing**: Ensure reliability with extensive unit and integration tests

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Processing    │    │   Data Storage  │
│                 │    │                 │    │                 │
│  ┌─────────────┐│    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│  │ NOAA API    ││───▶│ │ PySpark ETL │ │───▶│ │ Parquet     │ │
│  │ CSV Files   ││    │ │ Pipeline    │ │    │ │ Files       │ │
│  └─────────────┘│    │ └─────────────┘ │    │ └─────────────┘ │
│                 │    │                 │    │                 │
│                 │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│                 │    │ │ Data        │ │    │ │ SQL Views   │ │
│                 │    │ │ Validation  │ │    │ │ & Analytics │ │
│                 │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

1. **Ingestion** - Download weather data from NOAA stations
2. **Transformation** - Clean, validate, and aggregate data using PySpark
3. **Storage** - Save processed data in partitioned Parquet format
4. **Analytics** - Create SQL views for business intelligence

## Quick Start

### Prerequisites

```bash
# Python 3.11+ and Java 8+ required
python3 --version
java -version

# Install dependencies
pip install -r requirements.txt
```

### Running the Pipeline

```bash
# Complete pipeline execution
python3 main.py --ingest --transform --station-id GHCND:USW00014735

# Individual components
python3 main.py --ingest-only --start-date 2024-01-01 --end-date 2024-01-31
python3 main.py --transform-only --input-file data/landing/weather_2024-01-01_2024-01-31.csv

# Using Spark Submit (recommended for production)
spark-submit --master local[*] main.py --ingest --transform
```

### Using the Convenience Script

```bash
# Run complete pipeline
./run_pipeline.sh full

# Run with custom parameters
./run_pipeline.sh custom --start-date 2024-01-01 --end-date 2024-12-31

# Performance testing
./run_pipeline.sh performance
```

## Example Usage

### 1. Basic Pipeline Execution

```python
from main import run_full_pipeline

# Run complete ETL pipeline
run_full_pipeline(
    station_id="GHCND:USW00014735",  # JFK Airport
    start_date="2024-01-01",
    end_date="2024-12-31",
    landing_dir="data/landing",
    processed_dir="data/processed"
)
```

### 2. Custom Data Processing

```python
from transform import create_spark_session, transform_weather_data

# Create Spark session
spark = create_spark_session("CustomWeatherAnalysis")

# Process specific dataset
transform_weather_data(
    input_path="data/landing/weather_data.csv",
    output_path="data/processed/weather_analysis",
    spark_session=spark
)
```

### 3. Spark SQL Analytics

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WeatherAnalytics").getOrCreate()

# Load processed data
df = spark.read.parquet("data/processed/weather_data")

# Create temporary view
df.createOrReplaceTempView("weather_analytics")

# Example SQL queries
spark.sql("""
    SELECT 
        year, month,
        AVG(avg_temperature) as monthly_avg_temp,
        SUM(total_precipitation) as monthly_precipitation,
        COUNT(*) as days_with_data
    FROM weather_analytics 
    WHERE avg_temperature IS NOT NULL
    GROUP BY year, month
    ORDER BY year, month
""").show()

# Weather extremes analysis
spark.sql("""
    SELECT 
        date,
        station,
        avg_temperature,
        total_precipitation,
        CASE 
            WHEN avg_temperature > 30 THEN 'Hot'
            WHEN avg_temperature < 0 THEN 'Freezing'
            ELSE 'Normal'
        END as temperature_category
    FROM weather_analytics
    WHERE total_precipitation > 50 OR avg_temperature > 35 OR avg_temperature < -10
    ORDER BY date DESC
""").show()
```

## Testing

### Running Tests

```bash
# Run all tests
./run_tests.sh all

# Run specific test categories
./run_tests.sh unit           # Unit tests only
./run_tests.sh integration    # Integration tests only
./run_tests.sh fast           # Fast tests (exclude slow)

# Run with coverage
./run_tests.sh coverage

# Validate test environment
./run_tests.sh validate
```

### Test Categories

- **Unit Tests** (26 tests): Individual function testing
- **Integration Tests** (7 tests): End-to-end pipeline validation
- **Performance Tests**: Processing time benchmarks
- **Edge Case Tests**: Error handling and extreme values

## Project Structure

```
weather-spark-pipeline/
├── Core Pipeline
│   ├── ingest.py              # Data ingestion from NOAA
│   ├── transform.py           # PySpark transformations
│   └── main.py                # Pipeline orchestration
├── Testing
│   ├── tests/
│   │   ├── test_ingest.py     # Ingestion tests
│   │   ├── test_transform.py  # Transformation tests
│   │   ├── test_integration.py # End-to-end tests
│   │   ├── test_simple_units.py # Focused unit tests
│   │   └── test_spark_sql.py  # Spark SQL tests
│   ├── pytest.ini            # Test configuration
│   └── run_tests.sh          # Test runner script
├── Data Directories
│   ├── data/
│   │   ├── landing/          # Raw CSV files
│   │   ├── processed/        # Processed Parquet files
│   │   └── output/           # Final output
├── Configuration
│   ├── requirements.txt      # Python dependencies
│   ├── .gitignore           # Git ignore rules
│   └── .vscode/             # IDE configuration
├── Examples
│   └── spark_sql_analytics.py # Spark SQL examples
└── Documentation
    ├── README.md            # This file
    └── run_pipeline.sh      # Execution script
```

## Configuration

### Environment Variables

```bash
# Optional configuration
export SPARK_HOME="/path/to/spark"
export JAVA_HOME="/path/to/java"
export PYTHONPATH="${PYTHONPATH}:${PWD}"

# Logging level
export LOG_LEVEL="INFO"  # DEBUG, INFO, WARNING, ERROR
```

### Spark Configuration

```python
# Customize Spark settings in transform.py
spark = SparkSession.builder \
    .appName("WeatherETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
    .getOrCreate()
```

## Performance Optimization

### Recommended Spark Settings

```bash
# For large datasets
spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 2g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    main.py --ingest --transform
```

### Data Partitioning

Data is automatically partitioned by year and month for optimal query performance:

```
data/processed/
├── year=2024/
│   ├── month=1/
│   │   └── *.parquet
│   └── month=2/
│       └── *.parquet
```

## Error Handling

The pipeline includes comprehensive error handling:

- **Data Validation**: Checks for missing or invalid data
- **Schema Validation**: Ensures data consistency
- **Graceful Degradation**: Continues processing with warnings
- **Detailed Logging**: Comprehensive error reporting

## Production Deployment

### Docker Deployment

```dockerfile
FROM apache/spark-py:v3.5.0

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . /app
WORKDIR /app

CMD ["python", "main.py", "--ingest", "--transform"]
```

### Kubernetes Deployment

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: weather-etl-pipeline
spec:
  template:
    spec:
      containers:
      - name: weather-etl
        image: weather-spark-pipeline:latest
        command: ["python", "main.py"]
        args: ["--ingest", "--transform"]
```

## Monitoring and Observability

### Logging

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
```

### Metrics

- Processing time tracking
- Data quality metrics
- Error rate monitoring
- Resource utilization

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Run tests: `./run_tests.sh all`
4. Commit changes: `git commit -m 'Add new feature'`
5. Push to branch: `git push origin feature/new-feature`
6. Open a Pull Request

## Requirements

### System Requirements

- **Python**: 3.11+
- **Java**: 8 or 11 (for Spark)
- **Memory**: 4GB+ RAM recommended
- **Storage**: 1GB+ available space

### Python Dependencies

```
pyspark==3.5.0
requests==2.31.0
pandas==2.1.3
pytest==7.4.3
```

## License

This project is licensed under the MIT License.

## Acknowledgments

- **NOAA**: For providing comprehensive weather data
- **Apache Spark**: For powerful distributed computing capabilities
- **Python Community**: For excellent data processing libraries

---

**Professional ETL Pipeline Implementation**

*This project demonstrates enterprise-grade ETL pipeline development with modern testing practices, comprehensive documentation, and production-ready deployment configurations.*

## Code Quality

This project uses `flake8` for linting and `mypy` for static type checking. Run:

```bash
make lint
make typecheck
```

## Data Profiling

After each transformation, the pipeline generates a simple data profile report (`data/profile_report.md`) with row counts, nulls, and min/max per column. This helps validate data quality and is useful for debugging and reporting.

## Makefile & Linting

A `Makefile` is provided for common tasks:
- `make test` – run all tests
- `make lint` – check code style with flake8
- `make run` – run the full pipeline
- `make profile` – run transformation and show the profile report
- `make clean` – remove all generated data and logs

Code style is enforced with `flake8` (see `.flake8` config).

## What I Learned

- Building robust ETL pipelines with PySpark
- Writing professional, testable, and maintainable code
- Implementing data validation and profiling
- Using CI/CD and automation for code quality
- Documenting and structuring a real-world data engineering project 
 