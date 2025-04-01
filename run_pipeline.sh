#!/bin/bash

# MovieLens Pipeline Runner
# This script runs the complete MovieLens data pipeline

# Setup logging
log_file="pipeline_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$log_file") 2>&1

echo "=== MovieLens Pipeline Started at $(date) ==="
echo ""

# Function to run and time a command
run_step() {
  local step_name="$1"
  local command="$2"
  
  echo "--- Starting: $step_name ---"
  start_time=$(date +%s)
  
  # Run the command
  eval "$command"
  status=$?
  
  end_time=$(date +%s)
  duration=$((end_time - start_time))
  
  if [ $status -eq 0 ]; then
    echo "--- Completed: $step_name (Time: ${duration}s) ---"
  else
    echo "--- FAILED: $step_name (Time: ${duration}s) ---"
    echo "Error running command: $command"
    exit 1
  fi
  echo ""
}

# Phase 1: Process data through medallion architecture
run_step "Process Bronze Layer" "python3 iceberg/local_medallion.py --layer bronze"
run_step "Process Silver Layer" "python3 iceberg/local_medallion.py --layer silver"
run_step "Process Gold Layer" "python3 iceberg/local_medallion.py --layer gold"

# Phase 2: Upload to S3
run_step "Upload to S3" "python3 iceberg/s3_upload.py"

# Phase 3: Setup Spark and process analytics
run_step "Setup Spark" "python3 spark/setup_spark.py"
run_step "Process Analytics" "python3 spark/process_analytics.py"

# Phase 4: Prepare Data Warehouse
run_step "Query Data for DWH" "python3 dwh/query_data.py"

# Phase 5: Generate Dashboard
run_step "Generate Dashboard" "python3 dashboard/generate_dashboard.py"

echo "=== MovieLens Pipeline Completed at $(date) ==="
echo "Log file: $log_file"
echo ""
echo "Next steps:"
echo "1. Check the generated dashboard at: $(pwd)/output/dashboard/index.html"
echo "2. Connect Metabase to the processed data"
echo "3. Develop additional analytics or visualizations as needed"
