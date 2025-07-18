#!/bin/bash

# Weather ETL Pipeline Runner
# This script demonstrates how to run the weather ETL pipeline with various configurations

set -e

echo "Weather ETL Pipeline Runner"
echo "============================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if PySpark is available
check_spark() {
    if ! command -v spark-submit &> /dev/null; then
        print_error "spark-submit not found. Please install Apache Spark."
        exit 1
    fi
    
    if ! python -c "import pyspark" &> /dev/null; then
        print_error "PySpark not found. Please install: pip install pyspark"
        exit 1
    fi
    
    print_info "Spark environment verified"
}

# Install dependencies
install_deps() {
    print_info "Installing Python dependencies..."
    pip install -r requirements.txt
}

# Run tests
run_tests() {
    print_info "Running unit tests..."
    python -m pytest tests/ -v
}

# Run with different configurations
run_full_pipeline() {
    print_info "Running full ETL pipeline with Python..."
    python main.py \
        --station-id GHCND:USW00014735 \
        --start-date 2024-01-01 \
        --end-date 2024-01-31 \
        --log-level INFO
}

run_with_spark_submit() {
    print_info "Running ETL pipeline with spark-submit..."
    spark-submit \
        --master local[*] \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        main.py \
        --station-id GHCND:USW00014735 \
        --start-date 2024-01-01 \
        --end-date 2024-01-31 \
        --log-level INFO
}

run_ingestion_only() {
    print_info "Running ingestion only..."
    python main.py \
        --mode ingest \
        --station-id GHCND:USW00014735 \
        --start-date 2024-01-01 \
        --end-date 2024-01-31
}

run_transformation_only() {
    print_info "Running transformation only..."
    
    # First check if we have input data
    if [ ! -f "data/landing/weather_data_*.csv" ]; then
        print_warning "No input data found. Running ingestion first..."
        run_ingestion_only
    fi
    
    # Find the CSV file
    csv_file=$(find data/landing -name "weather_data_*.csv" | head -1)
    
    if [ -z "$csv_file" ]; then
        print_error "No CSV file found in data/landing/"
        exit 1
    fi
    
    python main.py \
        --mode transform \
        --input-path "$csv_file"
}

run_spark_cluster() {
    print_info "Running on Spark cluster (requires cluster setup)..."
    spark-submit \
        --master spark://localhost:7077 \
        --executor-memory 2g \
        --total-executor-cores 4 \
        --conf spark.sql.adaptive.enabled=true \
        main.py \
        --station-id GHCND:USW00014735 \
        --start-date 2024-01-01 \
        --end-date 2024-01-31
}

# Performance testing
run_performance_test() {
    print_info "Running performance test with larger dataset..."
    python main.py \
        --station-id GHCND:USW00014735 \
        --start-date 2023-01-01 \
        --end-date 2024-12-31 \
        --log-level INFO
}

# Clean up function
cleanup() {
    print_info "Cleaning up temporary files..."
    rm -rf data/landing/*.csv
    rm -rf data/processed/weather_parquet
    rm -f pipeline.log
    print_info "Cleanup completed"
}

# Show usage
usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  check-spark      Check if Spark is properly installed"
    echo "  install-deps     Install Python dependencies"
    echo "  test             Run unit tests"
    echo "  run-full         Run full ETL pipeline with Python"
    echo "  run-spark        Run ETL pipeline with spark-submit"
    echo "  run-ingest       Run ingestion only"
    echo "  run-transform    Run transformation only"
    echo "  run-cluster      Run on Spark cluster"
    echo "  run-perf         Run performance test"
    echo "  cleanup          Clean up temporary files"
    echo "  help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 check-spark"
    echo "  $0 install-deps"
    echo "  $0 test"
    echo "  $0 run-full"
    echo "  $0 run-spark"
    echo ""
    echo "Spark Submit Examples:"
    echo "  # Basic local run"
    echo "  spark-submit --master local[*] main.py"
    echo ""
    echo "  # With configuration"
    echo "  spark-submit --master local[*] \\"
    echo "    --conf spark.sql.adaptive.enabled=true \\"
    echo "    --conf spark.sql.adaptive.coalescePartitions.enabled=true \\"
    echo "    main.py --station-id GHCND:USW00014735"
    echo ""
    echo "  # With resource allocation"
    echo "  spark-submit --master local[*] \\"
    echo "    --driver-memory 4g \\"
    echo "    --executor-memory 2g \\"
    echo "    main.py --start-date 2024-01-01 --end-date 2024-12-31"
}

# Main script logic
case "${1:-help}" in
    check-spark)
        check_spark
        ;;
    install-deps)
        install_deps
        ;;
    test)
        run_tests
        ;;
    run-full)
        check_spark
        run_full_pipeline
        ;;
    run-spark)
        check_spark
        run_with_spark_submit
        ;;
    run-ingest)
        run_ingestion_only
        ;;
    run-transform)
        run_transformation_only
        ;;
    run-cluster)
        run_spark_cluster
        ;;
    run-perf)
        check_spark
        run_performance_test
        ;;
    cleanup)
        cleanup
        ;;
    help)
        usage
        ;;
    *)
        print_error "Unknown option: $1"
        usage
        exit 1
        ;;
esac

print_info "Operation completed successfully!" 