#!/usr/bin/env python3
"""
Simple CSV diagnostic script
"""

import csv
import os

def check_csv_file(file_path):
    """Check CSV file structure"""
    print(f"Checking file: {file_path}")
    print(f"File exists: {os.path.exists(file_path)}")
    
    if not os.path.exists(file_path):
        print("File does not exist!")
        return
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Read first few lines to detect delimiter
            sample = file.read(1024)
            file.seek(0)
            
            print(f"Sample content: {sample[:200]}...")
            print()
            
            # Try different delimiters
            delimiters = [';', ',', '\t']
            detected_delimiter = None
            
            for delimiter in delimiters:
                if delimiter in sample:
                    detected_delimiter = delimiter
                    break
            
            if detected_delimiter:
                print(f"Detected delimiter: '{detected_delimiter}'")
                reader = csv.DictReader(file, delimiter=detected_delimiter)
                
                print(f"Columns found: {reader.fieldnames}")
                print()
                
                # Show first few rows
                print("First 3 rows:")
                for i, row in enumerate(reader):
                    if i >= 3:
                        break
                    print(f"Row {i+1}:")
                    for key, value in row.items():
                        print(f"  {key}: {value}")
                    print()
            else:
                print("Could not detect delimiter!")
                
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    # Try the file path from your terminal output
    file_path = r"C:\Users\Vahidya\Desktop\doctorbazi sample\لیست ایدی هر مبحث 2 items.csv"
    check_csv_file(file_path)
    
    print("\n" + "="*50)
    print("Alternative file paths to try:")
    print(r"C:\Users\Vahidya\Desktop\doctorbazi sample\لیست ایدی هر مبحث.csv")
    print(r"C:\Users\Vahidya\Desktop\doctorbazi sample\لیست ایدی هر مبحث.csv")
