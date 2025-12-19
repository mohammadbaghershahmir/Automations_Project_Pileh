#!/usr/bin/env python3
"""Test script for third_stage_converter"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from third_stage_converter import convert_third_stage_file

# Test file path
test_file = "Bolognia 5th Edition 2024 1-7-26-46_final_output_Bolognia 5th Edition 2024 1-7-26-46_final_output_post_processed_corrected_raw.json"

if not os.path.exists(test_file):
    print(f"Error: Test file not found: {test_file}")
    sys.exit(1)

print(f"Testing converter with file: {test_file}")
print("=" * 60)

# Test conversion
result_path = convert_third_stage_file(
    input_path=test_file,
    book_id=1,
    chapter_id=1,
    start_index=1,
    output_path=None  # Auto-generate
)

if result_path:
    print(f"\n✓ Conversion successful!")
    print(f"Output file: {result_path}")
    
    # Check if file exists and show some stats
    if os.path.exists(result_path):
        import json
        with open(result_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"\nOutput statistics:")
        print(f"  - Total points: {data.get('metadata', {}).get('total_points', 0)}")
        print(f"  - Chapter: {data.get('metadata', {}).get('chapter', 'N/A')}")
        print(f"  - Book ID: {data.get('metadata', {}).get('book_id', 'N/A')}")
        print(f"  - Chapter ID: {data.get('metadata', {}).get('chapter_id', 'N/A')}")
        
        # Show first few points
        points = data.get('points', [])
        if points:
            print(f"\nFirst 3 points:")
            for i, point in enumerate(points[:3], 1):
                print(f"  {i}. PointId: {point.get('PointId', 'N/A')}")
                print(f"     Chapter: {point.get('chapter', 'N/A')}")
                print(f"     Subchapter: {point.get('subchapter', 'N/A')}")
                print(f"     Point: {point.get('points', 'N/A')[:80]}...")
else:
    print("\n✗ Conversion failed!")
    sys.exit(1)


