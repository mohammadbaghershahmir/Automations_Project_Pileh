# Content Automation Project

A comprehensive multi-stage content processing system using Google's Gemini AI API. This application processes PDF documents through a sophisticated pipeline that extracts, structures, and enriches content using AI-powered transformations.

## 🔒 Security

**IMPORTANT**: Please read [SECURITY_GUIDE.md](SECURITY_GUIDE.md) before using this application to learn how to protect your API keys from being leaked.

## Overview

This project provides an automated content processing pipeline that transforms PDF documents into structured JSON data through multiple AI-powered stages. The system is designed to handle large documents by splitting them into parts, processing each part independently, and then combining the results into a unified output. The pipeline extends beyond initial processing to include image notes, flashcards, test generation, and comprehensive content enrichment.

### Key Features

- **Multi-Stage Processing Pipeline**: Eleven-stage processing system (Stages 1-4, E, F, J, H, V, M, L) for comprehensive content transformation
- **Part-Based Processing**: Automatically splits large PDFs into manageable parts
- **Flexible Model Selection**: Choose different Gemini models for each processing stage
- **Automatic API Key Rotation**: Seamlessly rotates through multiple API keys
- **Robust Error Handling**: Retry mechanisms and comprehensive error recovery
- **PointId Generation**: Automatic generation of unique identifiers for content points
- **Modern GUI**: User-friendly tab-based interface built with CustomTkinter
- **Pipeline Status Tracking**: Real-time status bar showing progress of all stages
- **Comprehensive Logging**: Detailed logs for debugging and monitoring
- **Word Document Processing**: Support for Word files (.docx, .doc) as input for test questions
- **Image Processing**: Automatic image note generation and file management
- **Flashcard Generation**: AI-powered flashcard creation from content
- **Test File Generation**: Automated test question generation

## Architecture

The application follows a modular architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    Main GUI (main_gui.py)                   │
│              Orchestrates the entire pipeline               │
│              Tab-based interface for all stages              │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ Multi-Part   │   │   Post-      │   │   Third      │
│ Processor    │   │   Processor  │   │   Stage      │
│              │   │              │   │   Converter   │
└──────────────┘   └──────────────┘   └──────────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ Stage E      │   │ Stage F      │   │ Stage J      │
│ Processor    │   │ Processor    │   │ Processor    │
└──────────────┘   └──────────────┘   └──────────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
                    ┌──────────────┐
                    │  Base Stage  │
                    │  Processor   │
                    └──────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │  API Layer   │
                    │ (api_layer.py)│
                    └──────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ Gemini API   │
                    └──────────────┘
```

## Processing Pipeline

The system implements an eleven-stage processing pipeline divided into two main phases:

### Phase 1: Initial Content Processing (Stages 1-4)

#### Stage 1: Initial PDF Processing

**Purpose**: Extract raw content from PDF and convert to structured JSON format.

**Process**:
- PDF is uploaded and validated
- Content is sent to Gemini API with user-defined prompt
- Large PDFs are automatically split into parts for batch processing
- Results are combined into a single JSON file (`final_output.json`)

**Output**: `final_output.json` containing:
```json
{
  "metadata": {...},
  "rows": [
    {
      "Part": 1,
      "Number": 1,
      "Content": "...",
      ...
    }
  ]
}
```

**Components**:
- `MultiPartProcessor`: Handles PDF splitting and batch processing
- `GeminiAPIClient.process_pdf_with_prompt_batch()`: API calls for PDF processing

#### Stage 2: Per-Part Post-Processing

**Purpose**: Apply additional processing to each Part independently.

**Process**:
- Takes Stage 1 JSON as input
- Groups rows by Part number
- Sends each Part's data to Gemini API with Stage 2 prompt
- Combines all Part responses into a single JSON file

**Output**: `*_stage2.json` containing processed data for all parts

**Components**:
- `MultiPartPostProcessor`: Manages per-part processing
- `MultiPartPostProcessor.process_final_json_by_parts_with_responses()`: Returns both combined JSON and individual Part responses

**Key Features**:
- Processes each Part independently
- Maintains Part-specific responses for use in Stage 3
- Automatic JSON extraction from model responses
- Combines all Part results into unified output

#### Stage 3: Structuring & Point Extraction

**Purpose**: Transform Stage 2 output into hierarchical structure with points.

**Process**:
- For each Part:
  - Takes Stage 1 JSON (Part-specific) and Stage 2 response (Part-specific)
  - Sends to Gemini API with Stage 3 prompt
  - Saves raw model response as TXT file (no JSON conversion yet)
- Raw responses are saved for use in Stage 4

**Output**: Multiple `*_part{N}_stage3.txt` files (one per Part)

**Components**:
- Direct API calls via `GeminiAPIClient.process_text()`
- Raw text storage for Stage 4 processing

**Key Features**:
- Part-specific processing using corresponding Stage 2 responses
- Raw text storage (no immediate JSON conversion)
- Retry mechanism for failed API calls
- Progress tracking per Part

#### Stage 4: Final Point Generation & Enrichment

**Purpose**: Generate final structured points with PointId assignment.

**Process**:
- For each Part:
  - Takes Stage 1 JSON (Part-specific) and Stage 3 output (Part-specific)
  - Sends to Gemini API with Stage 4 prompt
  - Saves raw model response as TXT file
- After all Parts:
  - Loads all Stage 4 TXT files
  - Extracts JSON from each TXT file using `txt_stage_json_utils`
  - Flattens hierarchical JSON to points using `ThirdStageConverter`
  - Assigns PointId sequentially to all points
  - Combines into final merged JSON

**Output**: 
- Individual: `*_part{N}_stage4.txt` files
- Final: `*_final_points.json` with all points and PointIds

**Components**:
- `txt_stage_json_utils.load_stage_txt_as_json()`: Robust JSON extraction from TXT files
- `ThirdStageConverter._flatten_to_points()`: Converts hierarchical JSON to flat point structure
- PointId generation: Sequential 10-digit IDs (format: `BBBCCCPPPP`)

**Key Features**:
- Robust JSON extraction from markdown code blocks
- Handles incomplete or malformed JSON
- Automatic PointId assignment
- Sequential processing across all Parts
- Final merged output with metadata

### Phase 2: Content Enrichment (Stages E, F, J, H, V, M, L)

#### Stage E: Image Notes Processing

**Purpose**: Generate image notes from Stage 4 data and merge with existing points.

**Process**:
- Takes Stage 4 JSON (with PointId) and Stage 1 JSON as input
- Sends data to Gemini API with image notes generation prompt
- Model generates 7-column JSON: `chapter, subchapter, topic, subtopic, subsubtopic, point_text, caption`
- Processes image notes:
  - Removes `caption` column from final output
  - Replaces `point_text` with `Points` column (from Stage 4)
  - Assigns sequential PointIds (continuing from last Stage 4 PointId)
- Saves original filepic data (with caption) to `*_filepic.json` for Stage F
- Merges processed image notes with Stage 4 data
- Adds marker to indicate where image notes start

**Output**: 
- `e{book_number}{chapter_number}.json` - Merged output with image notes
- `*_filepic.json` - Original image notes with caption (for Stage F)
- `*_stage_e.txt` - Raw model response

**Components**:
- `StageEProcessor`: Handles image notes generation and merging
- `BaseStageProcessor`: Common functionality for JSON handling

**Key Features**:
- Automatic PointId continuation from Stage 4
- Preserves original filepic data for downstream stages
- Marker for image notes start position
- Robust JSON extraction from model responses

#### Stage F: Image File Generation

**Purpose**: Generate JSON file for images with metadata and descriptions.

**Process**:
- Takes Stage E JSON as input
- Extracts image records starting from `first_image_point_id` marker
- For each image record:
  - Extracts `file_name` from `Points` column
  - Transforms file names:
    - "تصویر 30:19" → "Fig30_19"
    - "جدول 30:3" → "Table30_3"
  - Determines `image_type`:
    - `2` if "Fig" or "تصویر" in file_name
    - `3` if "Table" or "جدول" in file_name
  - Sets `display_level` to `6`
  - Extracts `description` from `filepic.json` caption column
  - Sets `question` and `is_title` to empty strings

**Output**: `f.json` containing:
```json
{
  "metadata": {...},
  "data": [
    {
      "point_id": "1050030015",
      "file_name": "Fig30_19",
      "image_type": 2,
      "display_level": 6,
      "description": "...",
      "question": "",
      "is_title": ""
    }
  ]
}
```

**Components**:
- `StageFProcessor`: Handles image file generation
- `WordFileProcessor`: Reads Word documents (not used in Stage F, but available)

**Key Features**:
- Automatic file name transformation (Persian to English)
- Image type detection from file names
- Description extraction from filepic data
- Robust caption extraction with multiple key variations

#### Stage J: Add Imp & Type

**Purpose**: Add importance (`Imp`) and type (`Type`) columns to Stage E data.

**Process**:
- Takes Stage E JSON, Word file (test questions), and prompt as input
- Reads Word file as plain text
- Sends Stage E data and Word content to Gemini API
- Model generates JSON with `PointId, Imp, Type` for each point
- Merges 6 columns from Stage E with 2 new columns (`Imp`, `Type`)
- Creates 8-column output

**Output**: `a{book_number}{chapter_number}.json` containing:
```json
{
  "metadata": {...},
  "data": [
    {
      "PointId": "1050030001",
      "chapter": "...",
      "subchapter": "...",
      "topic": "...",
      "subtopic": "...",
      "subsubtopic": "...",
      "Points": "...",
      "Imp": "high",
      "Type": "concept"
    }
  ]
}
```

**Components**:
- `StageJProcessor`: Handles Imp and Type addition
- `WordFileProcessor`: Reads Word documents as plain text

**Key Features**:
- Word document processing (plain text extraction)
- PointId-based merging
- Retry mechanism for model calls
- Comprehensive error handling

#### Stage H: Flashcard Generation

**Purpose**: Generate flashcards from Stage J and Stage F data.

**Status**: Coming soon

**Planned Process**:
- Takes Stage J JSON (without `Imp` column) and Stage F JSON as input
- Generates flashcards with questions and multiple choice answers
- Output: `ac{book_number}{chapter_number}.json` with 16 columns

#### Stage V: Test File Generation

**Purpose**: Generate test files from Stage J data and Word document.

**Status**: Coming soon

**Planned Process**:
- Two-step process:
  - Step 1: Generate initial test questions
  - Step 2: Refine and combine with QId mapping
- Output: `b.json` with test questions mapped to point_ids

#### Stage M: Topic ID List

**Purpose**: Extract unique topic IDs from Stage H (ac) file.

**Status**: Coming soon

**Planned Process**:
- Extracts unique `chapter`, `subchapter`, and `topic` combinations
- Output: `i{book_number}{chapter_number}.json`

#### Stage L: Chapter Overview

**Purpose**: Generate chapter overview from Stage J and Stage V data.

**Status**: Coming soon

**Planned Process**:
- Takes Stage J (a file) and Stage V (b file) as input
- Generates comprehensive chapter overview
- Output: `o{book_number}{chapter_number}.json`

## Project Structure

```
content_automation_project/
├── __init__.py                      # Package initialization
├── api_layer.py                     # API layer for Gemini integration
├── pdf_processor.py                 # PDF validation and processing
├── prompt_manager.py                 # Predefined prompt management
├── main_gui.py                      # Main GUI application (tab-based)
├── run.py                           # Application entry point
├── multi_part_processor.py          # Stage 1: Multi-part PDF processing
├── multi_part_post_processor.py     # Stage 2: Per-part post-processing
├── third_stage_converter.py         # Stage 3/4: JSON conversion and flattening
├── third_stage_chunk_processor.py   # Chunked processing for large inputs
├── txt_stage_json_utils.py          # Utilities for loading TXT files as JSON
├── base_stage_processor.py          # Base class for all stage processors
├── stage_e_processor.py             # Stage E: Image notes processing
├── stage_f_processor.py             # Stage F: Image file generation
├── stage_j_processor.py             # Stage J: Add Imp & Type
├── word_file_processor.py            # Word document processing utilities
├── test_converter.py                 # Testing utilities
├── requirements.txt                 # Python dependencies
├── prompts.json                     # Predefined prompts database
├── README.md                        # This file
├── SECURITY_GUIDE.md                # Security best practices
└── BUILD_INSTRUCTIONS.md            # Build instructions for executables
```

## Core Components

### API Layer (`api_layer.py`)

Provides a clean, abstracted interface for all Gemini API interactions.

#### APIConfig
Configuration class containing:
- Available TTS models: `gemini-2.5-flash-preview-tts`, `gemini-2.5-pro-preview-tts`
- Available text processing models: `gemini-2.5-flash`, `gemini-2.5-pro`, `gemini-2.0-flash`, `gemini-1.5-pro`, `gemini-1.5-flash`
- Available TTS voices: 30+ voice options
- Default settings: temperature, max tokens, etc.

#### APIKeyManager
Manages API keys with rotation support:
- Load API keys from CSV files (format: `account;project;api_key`)
- Automatic rotation through multiple keys
- Manual key addition
- Error message sanitization (removes API keys from error messages)

#### GeminiAPIClient
Main client for Gemini services:
- `process_pdf_with_prompt()`: Single PDF processing
- `process_pdf_with_prompt_batch()`: Batch PDF processing with automatic splitting
- `process_text()`: Text processing with prompts
- `generate_tts()`: Text-to-Speech generation (async and sync)
- Automatic API key rotation on errors
- Comprehensive error handling and retry logic

### Base Stage Processor (`base_stage_processor.py`)

Base class providing common functionality for all stage processors (E, F, J, H, V, M, L):

**Key Methods**:
- `load_json_file()`: Robust JSON file loading with error handling
- `save_json_file()`: Standard JSON saving with metadata structure
- `extract_json_from_response()`: JSON extraction from model responses
- `load_txt_as_json()`: Load TXT files and extract JSON
- `generate_filename()`: Generate filenames with book/chapter format
- `extract_book_chapter_from_pointid()`: Extract book and chapter from PointId
- `get_data_from_json()`: Extract data array from JSON structure
- `get_metadata_from_json()`: Extract metadata from JSON structure

**Benefits**:
- Consistent error handling across all stages
- Standardized JSON structure (metadata + data)
- Reusable utilities for common operations
- Centralized logging

### Multi-Part Processor (`multi_part_processor.py`)

Handles Stage 1 processing:
- Splits large PDFs into manageable parts
- Processes parts in batches
- Combines results into single JSON
- Deduplicates rows based on content
- Sorts rows by Number field

### Multi-Part Post-Processor (`multi_part_post_processor.py`)

Handles Stage 2 processing:
- Groups rows by Part number
- Processes each Part independently
- Extracts JSON blocks from model responses
- Combines all Part results
- Returns both combined JSON and individual Part responses

### Third Stage Converter (`third_stage_converter.py`)

Converts hierarchical JSON structures to flat point format:
- Extracts JSON from markdown code blocks
- Handles incomplete or malformed JSON
- Flattens nested structures to points
- Generates PointId for each point
- Supports both English and Persian keys

### TXT Stage JSON Utils (`txt_stage_json_utils.py`)

Robust utilities for loading Stage 3/4 TXT outputs as JSON:
- Handles markdown code fences (```json ... ```)
- Repairs incomplete JSON (unterminated strings, missing braces)
- Multiple extraction strategies
- Fallback mechanisms for edge cases
- Manual object extraction from incomplete arrays

### Word File Processor (`word_file_processor.py`)

Handles Word document processing:
- Reads `.docx` and `.doc` files
- Converts to plain text for model input
- Supports python-docx library (with fallback)
- Prepares content for model consumption

### Stage Processors

#### Stage E Processor (`stage_e_processor.py`)
- Image notes generation from Stage 4 and Stage 1 data
- PointId assignment continuation
- Filepic data preservation for Stage F
- Marker insertion for image notes start

#### Stage F Processor (`stage_f_processor.py`)
- Image file JSON generation
- File name transformation (Persian to English)
- Image type detection
- Description extraction from filepic data

#### Stage J Processor (`stage_j_processor.py`)
- Imp and Type column addition
- Word document integration
- PointId-based merging
- 8-column output generation

### Third Stage Chunk Processor (`third_stage_chunk_processor.py`)

Handles chunked processing for very large inputs:
- Processes data in chunks with cursor-based pagination
- Accumulates content across chunks
- Supports chunked JSON structure with `chunk_index`, `is_last`, `payload`, `next_cursor`
- Safety limits on maximum chunks

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 2: Prepare API Keys

Create a CSV file with your Gemini API keys:

**Format**: `account;project;api_key`

**Example** (`api_keys.csv`):
```csv
account;project;api_key
Account1;Project1;YOUR_API_KEY_1_HERE
Account2;Project2;YOUR_API_KEY_2_HERE
```

**Note**: Multiple API keys enable automatic rotation and better rate limit handling.

### Step 3: Run the Application

```bash
python run.py
```

## Usage Guide

### Basic Workflow

#### 1. Configure API Keys

1. Click "Browse" next to "API Key CSV File"
2. Select your CSV file containing API keys
3. Status will show "X API keys loaded" if successful

#### 2. Upload PDF

1. Click "Browse" next to "PDF File"
2. Select a PDF file (no page limit)
3. System will validate and show page count and file size

#### 3. Select Prompt

**Option A - Predefined Prompt**:
1. Select "Use Predefined Prompt"
2. Choose a prompt from the dropdown
3. Preview the prompt if needed

**Option B - Custom Prompt**:
1. Select "Use Custom Prompt"
2. Enter your prompt in the text area
3. Supports Farsi/Persian text

#### 4. Select Model

Choose a Gemini model from the dropdown:
- `gemini-2.5-flash` (fast, recommended for most cases)
- `gemini-2.5-pro` (more capable, slower)
- `gemini-2.0-flash`
- `gemini-1.5-pro`
- `gemini-1.5-flash`

#### 5. Process PDF (Stage 1)

1. Click "Process PDF with AI"
2. Wait for processing (status updates shown)
3. Stage 1 JSON (`final_output.json`) will be saved
4. Response will appear in a new window

### Advanced: Multi-Stage Pipeline

#### Stage 2: Per-Part Processing

1. Click "View Other Stages (E-L)" button to access the tabview
2. Navigate to "Stages 1-4" tab
3. In the Stages 2-3-4 form:
   - Enter Stage 2 prompt (applied to each Part)
   - Enter chapter name (replaces `{CHAPTER_NAME}` in prompt)
   - Select Stage 1 JSON file (auto-filled if available)
   - Select Stage 2 model
   - Click "Run Stage 2 Only (per Part)"
4. Stage 2 JSON (`*_stage2.json`) will be saved

#### Full Pipeline: Stages 2 to 3 to 4

1. In the "Stages 1-4" tab:
   - Configure Stage 2 settings (as above)
   - Enter Stage 3 prompt (structuring & point extraction)
   - Enter Stage 4 prompt (optional, e.g., question generation)
   - Select models for Stage 3 and Stage 4
   - Enter starting PointId (10 digits, e.g., `1050030000`)
   - Click "Run Full Pipeline (Stage 2 to 3 to 4 per Part)"

2. The system will:
   - Process Stage 2 for all Parts
   - Process Stage 3 for each Part (saves TXT files)
   - Process Stage 4 for each Part (saves TXT files)
   - Extract JSON from all Stage 4 TXT files
   - Flatten to points and assign PointIds
   - Merge into final JSON (`*_final_points.json`)

3. Progress is shown in real-time with progress bar

### Content Enrichment Stages

#### Stage E: Image Notes Processing

1. Navigate to "Stage E" tab in the tabview
2. Select Stage 4 JSON file (with PointId)
3. Select Stage 1 JSON file
4. Enter prompt for image notes generation
5. Select model
6. Click "Process Stage E"
7. Output: `e{book}{chapter}.json` and `*_filepic.json`

#### Stage F: Image File Generation

1. Navigate to "Stage F" tab
2. Stage E JSON file is auto-filled (if available)
3. Click "Process Stage F"
4. Output: `f.json` with image metadata

**Note**: File names are automatically transformed:
- "تصویر 30:19" → "Fig30_19"
- "جدول 30:3" → "Table30_3"

#### Stage J: Add Imp & Type

1. Navigate to "Stage J" tab
2. Select Stage E JSON file
3. Select Word file containing test questions
4. Enter prompt for Imp & Type generation
5. Select model
6. Click "Process Stage J"
7. Output: `a{book}{chapter}.json` with 8 columns (6 from Stage E + Imp + Type)

### Pipeline Status Bar

The pipeline status bar at the top shows the status of all enrichment stages (E, F, J, H, V, M, L):
- **Waiting**: Stage not yet processed
- **Processing**: Stage currently running
- **OK**: Stage completed successfully
- **Error**: Stage encountered an error

### Viewing Results

- **View JSON**: Click "View/Edit Stage 1 JSON" or use response window
- **Export CSV**: Use "Export to CSV" button (if available)
- **Check Logs**: View `content_automation.log` for detailed processing logs

## PointId Format

PointIds are 10-digit numbers with the format: `BBBCCCPPPP`

- **BBB** (3 digits): Book ID (e.g., 105)
- **CCC** (3 digits): Chapter ID (e.g., 003)
- **PPPP** (4 digits): Point index (sequential, e.g., 0001, 0002, ...)

**Example**: `1050030001` = Book 105, Chapter 3, Point 1

## File Naming Conventions

### Phase 1 Outputs

#### Stage 1 Output
- `{pdf_name}_final_output.json`

#### Stage 2 Output
- `{base_name}_stage2.json`

#### Stage 3 Output (per Part)
- `{base_name}_part{N}_stage3.txt`

#### Stage 4 Output (per Part)
- `{base_name}_part{N}_stage4.txt`

#### Final Merged Output
- `{base_name}_final_points.json`

### Phase 2 Outputs

#### Stage E Output
- `e{book_number}{chapter_number}.json` - Merged output with image notes
- `{base_name}_filepic.json` - Original image notes with caption

#### Stage F Output
- `f.json` - Image file JSON

#### Stage J Output
- `a{book_number}{chapter_number}.json` - Stage E data with Imp and Type columns

#### Stage H Output (Planned)
- `ac{book_number}{chapter_number}.json` - Flashcards with 16 columns

#### Stage V Output (Planned)
- `b.json` - Test questions with QId mapping

#### Stage M Output (Planned)
- `i{book_number}{chapter_number}.json` - Topic ID list

#### Stage L Output (Planned)
- `o{book_number}{chapter_number}.json` - Chapter overview

## Error Handling & Retry Logic

The system includes comprehensive error handling:

- **API Key Rotation**: Automatically switches to next key on errors
- **Retry Mechanisms**: 
  - Stage 3: Up to 2 retries per Part
  - Stage 4: Up to 2 retries per Part
  - Stage E: Up to 3 retries for model calls
  - Stage J: Up to 3 retries for model calls
- **Error Recovery**: Continues processing other Parts if one fails
- **Logging**: All errors are logged with full context
- **User Feedback**: Clear error messages in GUI
- **JSON Extraction Fallbacks**: Multiple strategies for extracting JSON from model responses

## Dependencies

### Core Dependencies

- `google-genai` - For TTS functionality
- `google-generativeai` - For text processing and PDF handling
- `PyPDF2` - For PDF validation and text extraction
- `customtkinter` - For modern GUI
- `Pillow` - For image processing support
- `python-docx` (optional) - For Word document processing (with fallback)

### Standard Library

- `csv` - CSV file handling
- `asyncio` - Async operations
- `wave` - Audio file handling
- `logging` - Comprehensive logging
- `json` - JSON processing
- `threading` - Background processing
- `pathlib` - Path operations
- `re` - Regular expressions
- `datetime` - Timestamp generation

## API Layer Usage Example

```python
from api_layer import APIKeyManager, GeminiAPIClient, APIConfig

# Initialize
key_manager = APIKeyManager()
key_manager.load_from_csv("api_keys.csv")

client = GeminiAPIClient(key_manager)

# Process PDF with prompt
response = client.process_pdf_with_prompt(
    pdf_path="document.pdf",
    prompt="Extract all key points from this document",
    model_name="gemini-2.5-flash"
)

# Process text
response = client.process_text(
    text="Your text here",
    system_prompt="Your prompt here",
    model_name="gemini-2.5-pro",
    temperature=0.7,
    max_tokens=16384
)
```

## Logging

The application creates detailed logs in `content_automation.log`:

- All API calls and responses
- Processing progress for each stage
- Error messages and stack traces
- Part processing status
- JSON extraction and conversion operations
- Stage E, F, J processing details

Log level can be adjusted in `main_gui.py` (`setup_logging()` method).

## Best Practices

1. **API Keys**: Use multiple API keys for better rate limit handling
2. **Model Selection**: Use `gemini-2.5-flash` for faster processing, `gemini-2.5-pro` for complex tasks
3. **Prompts**: Be specific and clear in your prompts for better results
4. **PointId**: Plan your PointId ranges to avoid conflicts across chapters
5. **Monitoring**: Check logs regularly to catch issues early
6. **Backup**: Keep backups of intermediate JSON files
7. **Word Files**: Ensure Word files are properly formatted and readable
8. **File Naming**: Follow the naming conventions for easier file management

## Troubleshooting

### Common Issues

**Issue**: "No API keys loaded"
- **Solution**: Check CSV file format (must be `account;project;api_key`)

**Issue**: "Stage 2 processing failed"
- **Solution**: Check Stage 1 JSON format, verify prompt is valid

**Issue**: "Failed to extract JSON from TXT"
- **Solution**: Check model response format, may need to adjust prompt

**Issue**: "PointId assignment failed"
- **Solution**: Verify PointId format (must be 10 digits)

**Issue**: "Stage F descriptions are empty"
- **Solution**: Ensure Stage E saved filepic.json correctly, check caption field exists

**Issue**: "Word file cannot be read"
- **Solution**: Ensure file is not corrupted, try converting to .docx format

### Debug Mode

Enable detailed logging by modifying `setup_logging()` in `main_gui.py`:
```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Security Considerations

- **Never commit API keys**: Use `.gitignore` to exclude CSV files with keys
- **Sanitize errors**: API keys are automatically removed from error messages
- **Read SECURITY_GUIDE.md**: Follow security best practices
- **Rotate keys**: Regularly rotate API keys for better security

## Contributing

When contributing to this project:

1. Follow existing code style (English naming, clear comments)
2. Add logging for new features
3. Update this README for significant changes
4. Test with various PDF sizes and formats
5. Ensure error handling is comprehensive
6. Use BaseStageProcessor for new stage processors
7. Follow the standard JSON structure (metadata + data)

## License

[Add your license information here]

## Support

For issues, questions, or contributions, please refer to the project repository.

---

**Last Updated**: December 2024
**Version**: 3.0 (Multi-Stage Pipeline with Content Enrichment)
