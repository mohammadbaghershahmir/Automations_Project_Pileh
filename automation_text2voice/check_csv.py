#!/usr/bin/env python3
"""
Diagnostic script to check CSV file structure
"""

import csv
import sys

def check_csv_structure(file_path):
    """Check the structure of a CSV file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Try to detect delimiter
            sample = file.read(1024)
            file.seek(0)
            
            # Check for semicolon delimiter first
            if ';' in sample:
                reader = csv.DictReader(file, delimiter=';')
            else:
                reader = csv.DictReader(file, delimiter=',')
            
            # Get fieldnames
            fieldnames = reader.fieldnames
            print(f"File: {file_path}")
            print(f"Delimiter: {';' if ';' in sample else ','}")
            print(f"Columns found: {fieldnames}")
            print()
            
            # Show first few rows
            print("First 3 rows:")
            for i, row in enumerate(reader):
                if i >= 3:
                    break
                print(f"Row {i+1}: {dict(row)}")
                print()
            
            return fieldnames
            
    except Exception as e:
        print(f"Error reading {file_path}: {str(e)}")
        return None

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        check_csv_structure(file_path)
    else:
        print("Usage: python check_csv.py <csv_file_path>")
        print("Example: python check_csv.py 'C:/Users/Vahidya/Desktop/doctorbazi sample/لیست ایدی هر مبحث 2 items.csv'")
