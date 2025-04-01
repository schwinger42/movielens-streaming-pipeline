#!/bin/bash

# Change to the dashboard directory
cd ~/movielens-pipeline/output/dashboard

# Start a simple HTTP server on port 8081 instead of 8080
python3 -m http.server 8081
