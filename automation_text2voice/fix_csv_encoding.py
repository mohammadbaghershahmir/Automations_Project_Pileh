#!/usr/bin/env python3
"""
Utility script to fix CSV encoding for Excel compatibility
Converts UTF-8 CSV files to UTF-8 with BOM so Excel can properly display Farsi text
"""

import os
import csv
import glob
import shutil
from datetime import datetime

def fix_csv_encoding(directory="."):
    """Fix CSV encoding by adding BOM to UTF-8 files"""
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    
    if not csv_files:
        print("No CSV files found in the current directory.")
        return
    
    print(f"Found {len(csv_files)} CSV files to process...")
    
    for csv_file in csv_files:
        print(f"\nProcessing: {os.path.basename(csv_file)}")
        
        # Create backup
        backup_file = f"{csv_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            shutil.copy2(csv_file, backup_file)
            print(f"  📁 Created backup: {os.path.basename(backup_file)}")
        except Exception as e:
            print(f"  ⚠️  Could not create backup: {e}")
            continue
        
        # Read the current file
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                content = file.read()
            print(f"  ✅ Read file with UTF-8 encoding")
        except UnicodeDecodeError:
            try:
                with open(csv_file, 'r', encoding='utf-8-sig') as file:
                    content = file.read()
                print(f"  ✅ Read file with UTF-8-BOM encoding (already fixed)")
            except UnicodeDecodeError:
                print(f"  ❌ Could not read {csv_file} - skipping")
                continue
        
        # Write back with BOM
        try:
            with open(csv_file, 'w', encoding='utf-8-sig', newline='') as file:
                file.write(content)
            print(f"  ✅ Fixed: {os.path.basename(csv_file)} (now Excel-compatible)")
        except Exception as e:
            print(f"  ❌ Error fixing {csv_file}: {e}")
            # Restore from backup
            try:
                shutil.copy2(backup_file, csv_file)
                print(f"  🔄 Restored from backup")
            except Exception as restore_error:
                print(f"  ⚠️  Could not restore from backup: {restore_error}")

def test_excel_compatibility():
    """Test if the current CSV files are Excel-compatible"""
    csv_files = glob.glob("*.csv")
    
    if not csv_files:
        print("No CSV files found to test.")
        return
    
    print("Testing Excel compatibility of current CSV files...")
    
    for csv_file in csv_files:
        print(f"\nTesting: {csv_file}")
        
        try:
            # Try to read with different encodings
            with open(csv_file, 'rb') as file:
                raw_content = file.read(10)  # Read first 10 bytes
            
            if raw_content.startswith(b'\xef\xbb\xbf'):
                print(f"  ✅ Has BOM - Excel compatible")
            else:
                print(f"  ⚠️  No BOM - may show gibberish in Excel")
                
        except Exception as e:
            print(f"  ❌ Error testing {csv_file}: {e}")

if __name__ == "__main__":
    print("=== CSV Encoding Fix Utility ===")
    print("This utility fixes CSV files to be Excel-compatible by adding UTF-8 BOM.")
    print("It will create backups before making changes.\n")
    
    # Test current files
    test_excel_compatibility()
    
    print("\n" + "="*50)
    response = input("Do you want to fix all CSV files in the current directory? (y/n): ")
    
    if response.lower() in ['y', 'yes']:
        fix_csv_encoding()
        print("\n✅ All CSV files have been fixed for Excel compatibility!")
        print("You can now open them in Excel and the Farsi text should display correctly.")
    else:
        print("No changes made.")
