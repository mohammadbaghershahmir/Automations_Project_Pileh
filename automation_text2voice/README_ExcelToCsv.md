# Excel to CSV Converter (UTF-8)

A simple and user-friendly Python GUI application to convert Excel files (.xlsx, .xls) to CSV format with UTF-8 encoding.

## Features

- 🖥️ User-friendly graphical interface
- 📁 Folder selection with file browser
- 🔄 Batch conversion of all Excel files in a folder (including subdirectories)
- 📊 Supports .xlsx and .xls formats
- ✅ UTF-8 BOM encoding for proper character preservation (including Farsi, Arabic, etc.)
- 📝 Real-time progress logging
- ⚡ Multi-threaded processing (non-blocking GUI)

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements_excel_to_csv.txt
```

Or install manually:
```bash
pip install pandas openpyxl
```

## Usage

1. Run the application:
```bash
python excel_to_csv_converter.py
```

2. Click "Browse Input" to select a folder containing Excel files
3. Click "Browse Output" to select a folder where CSV files will be saved
4. Click "Convert Excel to CSV" to start the conversion
5. CSV files will be created in the selected output folder

## How It Works

- Select an input folder containing Excel files
- Select an output folder where CSV files will be saved
- Recursively searches for all .xlsx and .xls files in the selected input folder
- Converts each Excel file to CSV format with UTF-8 BOM encoding
- Saves all CSV files to the selected output folder
- Shows progress and conversion results in real-time

## Requirements

- Python 3.7+
- pandas (for Excel reading and CSV writing)
- openpyxl (for .xlsx file support)

## Notes

- Original Excel files are not modified or deleted
- CSV files are saved to the selected output folder
- The application processes all files in the selected input folder and its subdirectories
- Converts with UTF-8 BOM encoding to ensure proper display of Farsi, Arabic, and other international characters in Excel

