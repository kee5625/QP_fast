#!/usr/bin/env python3

"""
Script to convert CSV files to Parquet format
"""

import pandas as pd
from pathlib import Path
import argparse


def convert_csv_to_parquet(input_dir: str, output_dir: str, pattern: str = "*.csv"):
    """
    Convert all CSV files in input_dir to Parquet format in output_dir
    
    Args:
        input_dir: Directory containing CSV files
        output_dir: Directory to save Parquet files
        pattern: Glob pattern to match CSV files (default: "*.csv")
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all CSV files matching the pattern
    csv_files = list(input_path.glob(pattern))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir} matching pattern {pattern}")
        return
    
    print(f"Found {len(csv_files)} CSV files to convert")
    
    # Convert each CSV file to Parquet
    for csv_file in csv_files:
        print(f"Converting {csv_file.name}...", end=" ")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Create output filename (replace .csv with .parquet)
            parquet_file = output_path / csv_file.name.replace('.csv', '.parquet')
            
            # Write to Parquet format
            df.to_parquet(parquet_file, engine='pyarrow', compression='snappy', index=False)
            
            # Print file size comparison
            csv_size = csv_file.stat().st_size / (1024 * 1024)  # MB
            parquet_size = parquet_file.stat().st_size / (1024 * 1024)  # MB
            compression_ratio = (1 - parquet_size / csv_size) * 100
            
            print(f"✓ ({csv_size:.2f} MB -> {parquet_size:.2f} MB, {compression_ratio:.1f}% reduction)")
            
        except Exception as e:
            print(f"✗ Error: {e}")
    
    print(f"\nConversion complete! Parquet files saved to {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet format")
    parser.add_argument(
        "--input-dir",
        "-i",
        default="data",
        help="Input directory containing CSV files (default: data)"
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default="data_parquet",
        help="Output directory for Parquet files (default: data_parquet)"
    )
    parser.add_argument(
        "--pattern",
        "-p",
        default="*.csv",
        help="Glob pattern to match CSV files (default: *.csv)"
    )
    
    args = parser.parse_args()
    
    convert_csv_to_parquet(args.input_dir, args.output_dir, args.pattern)


if __name__ == "__main__":
    main()
