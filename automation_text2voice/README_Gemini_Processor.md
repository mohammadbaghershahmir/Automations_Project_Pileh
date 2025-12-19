# Gemini CSV Processor

A Python application that uses Google's Gemini AI API to process CSV files and generate educational content based on topics.

## Features

- **Interactive GUI**: User-friendly interface for file selection and processing
- **API Key Rotation**: Automatically rotates through multiple API keys to avoid rate limits
- **Comprehensive Logging**: Detailed logs of all API requests and responses
- **Progress Tracking**: Real-time progress bar during processing
- **Error Handling**: Robust error handling with detailed error messages
- **Multi-threading**: Background processing to keep the GUI responsive

## Requirements

- Python 3.7 or higher
- Required packages (see requirements.txt):
  - google-generativeai
  - tkinter (usually included with Python)
  - csv, os, sys, logging, datetime, threading (built-in modules)

## Installation

1. Clone or download this repository
2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Input Files Required

### 1. Study CSV File
Contains study materials and reference content. The application will use this data as context for generating responses.

### 2. Questions CSV File
Contains questions related to the topics. Used as additional context for content generation.

### 3. Prompt TXT File
A text file containing the prompt template. Must include the placeholder `topicshouldreplacehere` which will be replaced with actual topics from the output file.

Example prompt template:
```
You are an expert educational content creator. Based on the provided study materials and questions, please create comprehensive content for the topic: topicshouldreplacehere

Please include:
1. A detailed explanation of the topic
2. Key concepts and definitions
3. Examples and practical applications
4. Common questions and their answers
5. Summary points for review
```

### 4. Output File Name CSV
Contains the topics to be processed. Must have these columns:
- `TopicID`: Unique identifier for each topic (used as filename)
- `Topic`: The actual topic name (replaces `topicshouldreplacehere` in prompt)
- `Chapter`: Chapter information (logged for tracking)
- `Subchapter`: Subchapter information (logged for tracking)

Example format:
```csv
TopicID,Topic,Chapter,Subchapter
T001,Introduction to Python Programming,Programming Basics,Python Fundamentals
T002,Variables and Data Types,Programming Basics,Python Fundamentals
```

### 5. API Key List CSV
Contains the API keys for Gemini AI. Must have these columns:
- `account`: Email or account identifier
- `project`: Project name or identifier
- `api_key`: The actual API key

Example format:
```csv
row;account;project;api_key
1;user@example.com;Project A;AIzaSy...
2;user@example.com;Project B;AIzaSy...
```

## Usage

1. Run the application:
   ```bash
   python gemini_csv_processor.py
   ```

2. In the GUI:
   - Select all required input files using the "Browse" buttons
   - Choose the Gemini model (gemini-2.5-pro or gemini-2.5-flash)
   - Select an output directory for the generated CSV files
   - Click "Start Processing"

3. The application will:
   - Load all input files
   - Process each row in the output file name CSV
   - For each topic:
     - Replace `topicshouldreplacehere` with the actual topic
     - Use the next API key in rotation
     - Send request to Gemini AI
     - Save response to a CSV file named `{TopicID}.csv`
     - Log all details including API key used, account, project, chapter, subchapter, topic, and success/failure status

## Output

- **Individual CSV Files**: Each topic gets its own CSV file named after the TopicID
- **Log File**: Detailed log file with timestamp showing all processing details
- **Progress Tracking**: Real-time progress bar in the GUI

## Logging

The application creates detailed logs including:
- API key used (first 10 characters for security)
- Account and project information
- Chapter, subchapter, and topic being processed
- Success/failure status for each request
- Error messages if any issues occur

Log files are saved as `gemini_processor_YYYYMMDD_HHMMSS.log`

## Error Handling

The application handles various error scenarios:
- Missing or invalid input files
- API key authentication failures
- Network connectivity issues
- Invalid CSV formats
- Missing required columns

## Model Selection

- **gemini-2.5-pro**: More powerful model, suitable for complex content generation
- **gemini-2.5-flash**: Faster model, suitable for quick responses

## API Key Rotation

The application automatically rotates through API keys:
- Starts with the first API key
- Moves to the next key for each request
- Loops back to the first key when all keys have been used
- This helps avoid rate limits and distribute load

## Troubleshooting

### Common Issues:

1. **"No valid API keys found"**: Check your API key CSV file format
2. **"Failed to load prompt template"**: Ensure the prompt file contains the placeholder `topicshouldreplacehere`
3. **"No Topic found"**: Check your output file name CSV has a "Topic" column
4. **API authentication errors**: Verify your API keys are valid and have proper permissions

### Getting Help:

- Check the log file for detailed error messages
- Ensure all input files are in the correct format
- Verify API keys are valid and have sufficient quota

## Security Notes

- API keys are logged with only the first 10 characters visible
- Log files contain sensitive information - keep them secure
- Never share your API keys or log files publicly

## License

This project is provided as-is for educational and personal use.
