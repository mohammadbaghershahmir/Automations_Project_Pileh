# Gemini CSV Processor - Complete Application

## Overview
I have created a comprehensive Python application that uses Google's Gemini AI API to process CSV files and generate educational content based on topics. The application meets all your specified requirements.

## Files Created

### 1. `gemini_csv_processor.py` - Main Application
- **Interactive GUI**: User-friendly interface for file selection and processing
- **API Key Rotation**: Automatically rotates through multiple API keys
- **Comprehensive Logging**: Detailed logs of all API requests and responses
- **Progress Tracking**: Real-time progress bar during processing
- **Error Handling**: Robust error handling with detailed error messages
- **Multi-threading**: Background processing to keep the GUI responsive

### 2. `sample_prompt_template.txt` - Example Prompt Template
- Shows how to use the `topicshouldreplacehere` placeholder
- Demonstrates proper prompt structure for educational content generation

### 3. `sample_output_file_names.csv` - Example Output File Format
- Shows the required CSV structure with TopicID, Topic, Chapter, and Subchapter columns
- Demonstrates how topics will be processed

### 4. `README_Gemini_Processor.md` - Comprehensive Documentation
- Complete usage instructions
- File format requirements
- Troubleshooting guide
- Security notes

### 5. `test_processor.py` - Test Script
- Tests all functionality without making actual API calls
- Verifies file loading, API key rotation, and CSV creation
- Creates temporary test files and cleans them up

### 6. `run_processor.bat` - Windows Batch File
- Easy way to run the application on Windows
- Double-click to start the processor

## Key Features Implemented

### ✅ Input File Selection
- Study CSV file
- Questions CSV file  
- Prompt TXT file
- Output file name CSV file
- API key list CSV file

### ✅ Model Selection
- Choice between gemini-2.5-pro and gemini-2.5-flash
- Dropdown menu in the GUI

### ✅ Processing Loop
- Processes each row in the output file name CSV
- Replaces `topicshouldreplacehere` with actual Topic from each row
- Saves responses to CSV files named after TopicID values

### ✅ API Key Rotation
- Starts from first API key and cycles through all keys
- Loops back to beginning when all keys are used
- Logs which API key, account, and project are used for each request

### ✅ Comprehensive Logging
- API key used (first 10 characters for security)
- Account and project information
- Chapter, subchapter, and topic being processed
- Success/failure status for each request
- Detailed error messages

### ✅ Output Generation
- Creates individual CSV files for each topic
- Files named using TopicID values
- Progress tracking with real-time updates

## How to Use

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Application**:
   ```bash
   python gemini_csv_processor.py
   ```
   Or double-click `run_processor.bat` on Windows

3. **In the GUI**:
   - Select all required input files
   - Choose the Gemini model
   - Select output directory
   - Click "Start Processing"

4. **Monitor Progress**:
   - Watch the progress bar
   - Check the log file for detailed information
   - Find generated CSV files in your output directory

## File Format Requirements

### API Key List CSV (semicolon-delimited):
```csv
row;account;project;api_key
1;user@example.com;Project A;AIzaSy...
```

### Output File Names CSV:
```csv
TopicID,Topic,Chapter,Subchapter
T001,Introduction to Python,Basics,Fundamentals
```

### Prompt Template TXT:
Must contain `topicshouldreplacehere` placeholder that will be replaced with actual topics.

## Security Features

- API keys are logged with only first 10 characters visible
- Comprehensive error handling prevents data loss
- Input validation ensures file integrity
- Log files contain sensitive information - keep them secure

## Testing

Run the test script to verify functionality:
```bash
python test_processor.py
```

This will test all components without making actual API calls.

## Support

The application includes:
- Comprehensive error messages
- Detailed logging for troubleshooting
- Input validation
- Progress tracking
- Multi-threading for responsive GUI

All requirements from your specification have been implemented and tested successfully!
