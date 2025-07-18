#!/bin/bash

# Weather ETL Pipeline Test Runner
# This script runs different categories of tests for the weather ETL pipeline

set -e

echo "Weather ETL Pipeline Test Runner"
echo "================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

print_section() {
    echo -e "${BLUE}[SECTION]${NC} $1"
}

# Check if pytest is available
check_pytest() {
    if ! python3 -m pytest --version &> /dev/null; then
        print_error "pytest not found. Please install: pip install pytest"
        exit 1
    fi
    
    print_info "pytest is available"
}

# Run unit tests only
run_unit_tests() {
    print_section "Running Unit Tests"
    python3 -m pytest tests/ -v -m "unit" --tb=short
}

# Run integration tests only
run_integration_tests() {
    print_section "Running Integration Tests"
    python3 -m pytest tests/ -v -m "integration" --tb=short
}

# Run performance tests only
run_performance_tests() {
    print_section "Running Performance Tests"
    python3 -m pytest tests/ -v -m "performance" --tb=short
}

# Run all tests
run_all_tests() {
    print_section "Running All Tests"
    python3 -m pytest tests/ -v --tb=short
}

# Run fast tests only (excluding slow tests)
run_fast_tests() {
    print_section "Running Fast Tests"
    python3 -m pytest tests/ -v -m "not slow" --tb=short
}

# Run slow tests only
run_slow_tests() {
    print_section "Running Slow Tests"
    python3 -m pytest tests/ -v -m "slow" --tb=short
}

# Run tests with coverage
run_tests_with_coverage() {
    print_section "Running Tests with Coverage"
    
    # Check if coverage is available
    if ! python3 -c "import coverage" &> /dev/null; then
        print_warning "coverage not installed. Installing..."
        pip3 install coverage
    fi
    
    python3 -m pytest tests/ -v --tb=short \
        --cov=. \
        --cov-report=term-missing \
        --cov-report=html \
        --cov-exclude="tests/*"
    
    print_info "Coverage report generated in htmlcov/index.html"
}

# Run tests in parallel
run_tests_parallel() {
    print_section "Running Tests in Parallel"
    
    # Check if pytest-xdist is available
    if ! python3 -c "import xdist" &> /dev/null; then
        print_warning "pytest-xdist not installed. Installing..."
        pip3 install pytest-xdist
    fi
    
    python3 -m pytest tests/ -v --tb=short -n auto
}

# Run specific test file
run_specific_test() {
    local test_file=$1
    if [ -z "$test_file" ]; then
        print_error "Please specify a test file"
        exit 1
    fi
    
    print_section "Running Specific Test: $test_file"
    python3 -m pytest "tests/$test_file" -v --tb=short
}

# Run tests with detailed output
run_tests_verbose() {
    print_section "Running Tests with Verbose Output"
    python3 -m pytest tests/ -vv --tb=long -s
}

# Run tests and generate JUnit XML report
run_tests_junit() {
    print_section "Running Tests with JUnit XML Report"
    python3 -m pytest tests/ -v --tb=short --junitxml=test-results.xml
    
    if [ -f "test-results.xml" ]; then
        print_info "JUnit XML report generated: test-results.xml"
    fi
}

# Validate test setup
validate_test_setup() {
    print_section "Validating Test Setup"
    
    # Check if all required dependencies are installed
    print_info "Checking dependencies..."
    
    local deps=("pyspark" "pytest" "pandas" "requests")
    for dep in "${deps[@]}"; do
        if python3 -c "import $dep" &> /dev/null; then
            print_info "✓ $dep is installed"
        else
            print_error "✗ $dep is not installed"
            exit 1
        fi
    done
    
    # Check if test files exist
    print_info "Checking test files..."
    
    local test_files=("test_ingest.py" "test_transform.py" "test_integration.py")
    for file in "${test_files[@]}"; do
        if [ -f "tests/$file" ]; then
            print_info "✓ tests/$file exists"
        else
            print_error "✗ tests/$file not found"
            exit 1
        fi
    done
    
    print_info "Test setup validation completed successfully"
}

# Clean up test artifacts
cleanup_test_artifacts() {
    print_section "Cleaning Up Test Artifacts"
    
    # Remove test output directories
    rm -rf data/landing/*.csv
    rm -rf data/processed/*
    rm -rf .pytest_cache
    rm -rf htmlcov
    rm -f test-results.xml
    rm -f pipeline.log
    rm -f .coverage
    
    print_info "Test artifacts cleaned up"
}

# Show test statistics
show_test_stats() {
    print_section "Test Statistics"
    
    local total_tests=$(python3 -m pytest tests/ --collect-only -q | grep "test session starts" -A 100 | grep -E "^[0-9]+ tests collected" | awk '{print $1}')
    local unit_tests=$(python3 -m pytest tests/ --collect-only -q -m "unit" | grep -E "^[0-9]+ tests collected" | awk '{print $1}')
    local integration_tests=$(python3 -m pytest tests/ --collect-only -q -m "integration" | grep -E "^[0-9]+ tests collected" | awk '{print $1}')
    
    echo "Total tests: ${total_tests:-0}"
    echo "Unit tests: ${unit_tests:-0}"
    echo "Integration tests: ${integration_tests:-0}"
}

# Show usage
usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Test Options:"
    echo "  unit              Run unit tests only"
    echo "  integration       Run integration tests only"
    echo "  performance       Run performance tests only"
    echo "  all               Run all tests"
    echo "  fast              Run fast tests only (exclude slow tests)"
    echo "  slow              Run slow tests only"
    echo "  coverage          Run tests with coverage report"
    echo "  parallel          Run tests in parallel"
    echo "  verbose           Run tests with verbose output"
    echo "  junit             Run tests and generate JUnit XML report"
    echo ""
    echo "Utility Options:"
    echo "  validate          Validate test setup"
    echo "  cleanup           Clean up test artifacts"
    echo "  stats             Show test statistics"
    echo "  help              Show this help message"
    echo ""
    echo "Specific Test Options:"
    echo "  specific <file>   Run specific test file (e.g., test_ingest.py)"
    echo ""
    echo "Examples:"
    echo "  $0 unit"
    echo "  $0 integration"
    echo "  $0 coverage"
    echo "  $0 specific test_ingest.py"
    echo "  $0 fast"
}

# Main script logic
case "${1:-help}" in
    unit)
        check_pytest
        run_unit_tests
        ;;
    integration)
        check_pytest
        run_integration_tests
        ;;
    performance)
        check_pytest
        run_performance_tests
        ;;
    all)
        check_pytest
        run_all_tests
        ;;
    fast)
        check_pytest
        run_fast_tests
        ;;
    slow)
        check_pytest
        run_slow_tests
        ;;
    coverage)
        check_pytest
        run_tests_with_coverage
        ;;
    parallel)
        check_pytest
        run_tests_parallel
        ;;
    verbose)
        check_pytest
        run_tests_verbose
        ;;
    junit)
        check_pytest
        run_tests_junit
        ;;
    specific)
        check_pytest
        run_specific_test "$2"
        ;;
    validate)
        validate_test_setup
        ;;
    cleanup)
        cleanup_test_artifacts
        ;;
    stats)
        show_test_stats
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

print_info "Test execution completed!" 