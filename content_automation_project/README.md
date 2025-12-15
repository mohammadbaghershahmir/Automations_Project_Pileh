# Content Automation Project

Automated content processing and generation using Gemini AI.

## 🔒 Security

**IMPORTANT**: Please read [SECURITY_GUIDE.md](SECURITY_GUIDE.md) before using this application to learn how to protect your API keys from being leaked.

## Project Structure

```
content_automation_project/
├── __init__.py          # Package initialization
├── api_layer.py         # API layer for Gemini integration
├── pdf_processor.py     # PDF validation and processing
├── prompt_manager.py    # Predefined prompt management
├── main_gui.py          # Main GUI application
├── run.py               # Application entry point
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Features - Part 1

### PDF Upload
- Upload PDF files (no page limit)
- Automatic validation and page counting
- File size and page count display

### Prompt Management
- Use predefined prompts from dropdown menu
- Enter custom prompts manually
- Prompt preview for predefined prompts
- Default test prompt included

### Model Selection
- Choose from available Gemini models:
  - gemini-2.5-flash
  - gemini-2.5-pro
  - gemini-2.0-flash
  - gemini-1.5-pro
  - gemini-1.5-flash

### AI Studio Integration
- Send PDF and prompt to Gemini API
- Display AI response in separate window
- Automatic API key rotation
- Error handling and status updates

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Prepare API keys CSV file:
   - Format: `account;project;api_key`
   - Example:
     ```
     account;project;api_key
     Account1;Project1;YOUR_API_KEY_HERE
     ```

3. Run the application:
```bash
python run.py
```

## Usage

### Step 1: Configure API Keys
1. Click "Browse" next to "API Key CSV File"
2. Select your CSV file containing API keys
3. Status will show if keys loaded successfully

### Step 2: Upload PDF
1. Click "Browse" next to "PDF File"
2. Select a PDF file (no page limit)
3. System will validate and show page count

### Step 3: Select Prompt
- **Option A**: Choose from predefined prompts in dropdown
- **Option B**: Select "Use Custom Prompt" and enter your prompt

### Step 4: Select Model
- Choose desired Gemini model from dropdown

### Step 5: Process
- Click "🚀 Process PDF with AI"
- Wait for processing (status updates shown)
- Response will appear in a new window

## API Layer

The `api_layer.py` module provides a clean interface for interacting with Gemini API services.

### Components

#### APIConfig
Configuration class containing:
- Available TTS models
- Available text processing models
- Available TTS voices
- Default settings

#### APIKeyManager
Manages API keys with rotation support:
- Load API keys from CSV files
- Rotate through multiple keys
- Manual key addition

#### GeminiAPIClient
Main client for Gemini services:
- PDF processing with prompts
- Text-to-Speech generation (async and sync)
- Text processing with Gemini models
- Multi-speaker TTS support
- Automatic API key rotation

### Usage Example

```python
from api_layer import APIKeyManager, GeminiAPIClient

# Initialize
key_manager = APIKeyManager()
key_manager.load_from_csv("api_keys.csv")

client = GeminiAPIClient(key_manager)

# Process PDF with prompt
response = client.process_pdf_with_prompt(
    pdf_path="document.pdf",
    prompt="Summarize this document",
    model_name="gemini-2.5-flash"
)
```

## Dependencies

- `google-genai` - For TTS functionality
- `google-generativeai` - For text processing and PDF handling
- `PyPDF2` - For PDF validation and text extraction
- `customtkinter` - For modern GUI
- `Pillow` - For image processing support
- Standard library: `csv`, `asyncio`, `wave`, `logging`, `json`, `threading`

## Notes

- All naming and comments are in English
- API layer is separated from business logic
- Supports both async and sync operations
- PDF files are uploaded directly to Gemini API
- No page limit for PDF files

