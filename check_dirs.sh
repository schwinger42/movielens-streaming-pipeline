#!/bin/bash

# Function to check if a directory exists, create it if it doesn't
check_dir() {
    local dir=$1
    if [ ! -d "$dir" ]; then
        echo "Creating directory: $dir"
        mkdir -p "$dir"
    else
        echo "Directory exists: $dir"
    fi
}

# Function to check if a file exists
check_file() {
    local file=$1
    if [ -f "$file" ]; then
        echo "File exists: $file"
    else
        echo "File MISSING: $file"
    fi
}

echo "=== Checking Directory Structure ==="

# Check main directories
check_dir "spark"
check_dir "iceberg"
check_dir "dwh"
check_dir "dashboard"
check_dir "output"
check_dir "output/medallion/bronze"
check_dir "output/medallion/silver"
check_dir "output/medallion/gold"
check_dir "output/analysis"
check_dir "output/dwh"
check_dir "output/dashboard"

echo ""
echo "=== Checking Key Files ==="

# Check key files
check_file "spark/batch_processor_simple.py"
check_file "spark/setup_spark.py"
check_file "spark/process_analytics.py"
check_file "iceberg/local_medallion.py"
check_file "iceberg/s3_upload.py"
check_file "dwh/query_data.py"
check_file "dashboard/generate_dashboard.py"
check_file "run_pipeline.sh"

echo ""
echo "=== Directory Structure Summary ==="

echo "Total directories:"
find . -type d | grep -v "__pycache__" | grep -v ".git" | wc -l

echo "Total Python files:"
find . -name "*.py" | wc -l

echo ""
echo "=== Making Scripts Executable ==="
chmod +x run_pipeline.sh
echo "Made run_pipeline.sh executable"

echo ""
echo "Setup complete. You can now run the pipeline with: ./run_pipeline.sh"
