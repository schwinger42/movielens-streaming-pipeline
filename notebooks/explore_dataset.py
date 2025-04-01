import os
import pandas as pd

# Path to the dataset
dataset_path = 'raw/ml-20m'

# Check if dataset exists
if not os.path.exists(dataset_path):
    print(f"Dataset not found at {dataset_path}. Please run download_movielens.py first.")
    exit(1)

# List all files in the dataset
print("Files in the dataset:")
for file in os.listdir(dataset_path):
    file_path = os.path.join(dataset_path, file)
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"- {file} ({size_mb:.2f} MB)")

# Let's explore each CSV file
for file in os.listdir(dataset_path):
    if file.endswith('.csv'):
        file_path = os.path.join(dataset_path, file)
        print(f"\nExploring {file}:")
        
        try:
            # Read the first few rows
            df = pd.read_csv(file_path)
            
            # Print basic info
            print(f"Shape: {df.shape}")
            print("Columns:")
            for col in df.columns:
                print(f"- {col}")
            
            # Print sample data
            print("\nSample data:")
            print(df.head(3))
            
            # Print basic statistics for numeric columns
            if df.select_dtypes(include=['number']).shape[1] > 0:
                print("\nBasic statistics for numeric columns:")
                print(df.describe().iloc[:3])
            
        except Exception as e:
            print(f"Error exploring {file}: {e}")
        
        print("-" * 80)

print("Dataset exploration completed.")
