# Advanced Text-to-Speech with Gemini AI

A comprehensive Python application that combines Google's Gemini AI with both native Gemini TTS and Google Cloud Text-to-Speech services to create a powerful, user-friendly text-to-speech solution.

## ✨ Features

### 🎵 Dual TTS Services
- **Gemini Native TTS**: Direct audio generation using latest Gemini 2.5 TTS models (`gemini-2.5-flash-preview-tts`, `gemini-2.5-pro-preview-tts`)
- **Google Cloud TTS**: Traditional cloud-based service with extensive voice options

### 🎭 Multi-Speaker Support
- **Single Speaker**: Choose from 30+ premium voices (Kore, Puck, Zephyr, etc.)
- **Multi-Speaker**: Support for 2-speaker conversations with different voices
- **Dynamic Voice Assignment**: Automatically assign different voices to Speaker1 and Speaker2
- **Dialogue Format**: Smart parsing of speaker-labeled text

### 🤖 AI-Powered Text Enhancement
- Optional text preprocessing with Gemini AI models
- Customizable system prompts for text improvement
- Multiple Gemini models available (2.5-pro, 2.5-flash, 2.0-flash, etc.)

### 📝 Separate Context Control
- **Context Field**: Dedicated input for speaker instructions and dialogue setup
- **Smart Text Combination**: Automatically combines context with main text
- **Flexible Instructions**: Add character backgrounds, scene setting, or voice direction
- **Example Support**: Built-in examples for single and multi-speaker scenarios

### 🎛️ Advanced Audio Controls
- **Gemini TTS**: Natural language voice control through prompts and context
- **Google Cloud TTS**: Fine-grained control over speaking rate, pitch, and voice selection
- Multiple voice types: Standard, WaveNet, Neural2

### 🖥️ Modern User Interface
- Dark-themed GUI built with CustomTkinter
- Intuitive service selection with dynamic UI adaptation
- Multi-speaker toggle with voice assignment controls
- Built-in audio playback with pygame
- Comprehensive keyboard shortcuts (Ctrl+A, Ctrl+C, Ctrl+V, etc.)
- Smart placeholder text handling

### 💾 Settings & Configuration
- Persistent settings storage
- Multiple audio format support (.mp3, .wav)
- Easy API key configuration
- Service account support for Google Cloud TTS

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Gemini API key from [https://ai.google.dev/gemini-api](https://ai.google.dev/gemini-api)
- (Optional) Google Cloud service account for Google Cloud TTS

### Installation

1. **Clone or download the application files**

2. **Install dependencies** (run one of these):
   ```bash
   # Option 1: Use the smart launcher (recommended)
   python run.py
   
   # Option 2: Manual installation
   pip install -r requirements.txt
   ```

3. **Run the application**:
   ```bash
   # Option 1: Smart launcher with dependency checking
   python run.py
   
   # Option 2: Direct execution
   python app.py
   
   # Option 3: Windows batch file
   run.bat
   ```

### First Time Setup

1. **Get Gemini API Key**:
   - Visit [https://ai.google.dev/gemini-api](https://ai.google.dev/gemini-api)
   - Create an API key
   - Enter it in the "Gemini API Key" field

2. **Choose TTS Service**:
   - **Gemini Native TTS**: Uses your Gemini API key (latest feature!)
   - **Google Cloud TTS**: Requires Google Cloud setup (optional)

3. **Configure and Generate**:
   - Enter your text
   - Optionally enable AI text enhancement
   - Choose your preferred model
   - Click "Generate Speech"

## 📋 Dependencies

### Core Libraries
- `customtkinter>=5.2.0` - Modern UI framework
- `google-generativeai>=0.3.0` - Gemini AI text processing
- `google-genai>=0.1.0` - **New Gemini TTS library** (required for native TTS)
- `google-cloud-texttospeech>=2.14.1` - Google Cloud TTS (optional)
- `pygame>=2.1.2` - Audio playback
- `Pillow>=9.3.0` - Image processing support

### Recent Updates
- **Fixed Gemini TTS Implementation**: Now uses the correct Google GenAI SDK with Live API
- **Enhanced Error Handling**: Better error messages and automatic library installation
- **Improved Audio Output**: Gemini TTS now outputs proper WAV files
- **Keyboard Shortcuts**: Full text editing support with Ctrl+A, Ctrl+C, Ctrl+V, etc.

## 🎯 Detailed Usage Guide

### Service Selection
The application supports two TTS services:

#### Gemini Native TTS (Recommended)
- **Latest Technology**: Direct integration with Gemini 2.5 TTS models
- **Models Available**: 
  - `gemini-2.5-flash-preview-tts` (Fast, efficient)
  - `gemini-2.5-pro-preview-tts` (Higher quality)
- **Voice Control**: Use natural language in your text for voice characteristics
- **Output Format**: WAV files (24kHz, 16-bit, mono)

**Multi-Speaker Features**:
- **Single Speaker Mode**: Choose from 30+ voices (Kore, Puck, Zephyr, Fenrir, etc.)
- **Multi-Speaker Mode**: Assign different voices to Speaker1 and Speaker2
- **Automatic Voice Assignment**: Gemini automatically routes Speaker1/Speaker2 to different voices

#### Google Cloud TTS (Traditional)
- **Extensive Voice Library**: 200+ voices across 40+ languages
- **Voice Types**: Standard, WaveNet, Neural2
- **Fine Controls**: Precise speaking rate and pitch adjustment
- **Output Formats**: MP3 and WAV support

### Context and Multi-Speaker Usage

#### Single Speaker with Context
1. **Enable**: Leave "Multi-Speaker" unchecked
2. **Choose Voice**: Select your preferred voice (e.g., "Kore")
3. **Add Context**: Use the context field for instructions:
   ```
   Say this in a cheerful, upbeat voice like a podcast host
   ```
4. **Main Text**: Enter your content in the main text field

#### Multi-Speaker Conversations
1. **Enable**: Check "Enable Multi-Speaker (2 speakers)"
2. **Voice Assignment**: 
   - Speaker1 Voice: Choose voice for first speaker (e.g., "Kore")
   - Speaker2 Voice: Choose voice for second speaker (e.g., "Puck")
3. **Context Setup**: Use context field for scene setting:
   ```
   This is a conversation between two friends at a coffee shop.
   Make Speaker1 sound excited and Speaker2 sound calm.
   ```
4. **Dialogue Format**: Format your main text with speaker labels:
   ```
   Speaker1: Hey! How's your day going?
   Speaker2: Pretty good, thanks for asking. How about you?
   Speaker1: Fantastic! I just got some great news.
   ```

#### Advanced Context Examples

**Podcast Style**:
```
Context: This is a tech podcast. Speaker1 is the enthusiastic host, 
         Speaker2 is the expert guest. Keep it conversational.

Main Text:
Speaker1: Welcome to Tech Talk! Today we're discussing AI advancements.
Speaker2: Thanks for having me. It's an exciting time in the field.
```

**Storytelling**:
```
Context: Narrator and character dialogue for an audiobook.
         Speaker1 is the narrator, Speaker2 is a wise old character.

Main Text:
Speaker1: The old wizard looked up from his ancient tome.
Speaker2: Young one, you seek knowledge that comes with great responsibility.
```

**Educational Content**:
```
Context: Teacher-student interaction for educational content.
         Speaker1 is the patient teacher, Speaker2 is the curious student.

Main Text:
Speaker1: Today we'll learn about photosynthesis.
Speaker2: How do plants actually make their own food?
```

### Text Enhancement with AI
Enable the "Enhance text with Gemini AI" option to:
- Improve text clarity and flow for speech synthesis
- Add custom system prompts for specific processing instructions
- Use different Gemini models for text preprocessing
- Adjust temperature and token limits for AI generation

### Audio Configuration Fields

#### For Gemini Native TTS:
- **TTS Model**: Choose between flash (fast) or pro (quality)
- **Multi-Speaker Toggle**: Enable/disable 2-speaker mode
- **Voice Selection**: Single voice or dual voice assignment
- **Context Integration**: Separate field for instructions and scene setting

#### For Google Cloud TTS:
- **Voice Selection**: Choose from extensive voice library
- **Speaking Rate**: 0.25x to 4.0x normal speed
- **Pitch**: Adjust voice pitch from -20 to +20 semitones
- **Audio Format**: Select MP3 or WAV output

### Keyboard Shortcuts
- **Ctrl+A**: Select all text
- **Ctrl+C**: Copy selected text
- **Ctrl+V**: Paste text
- **Ctrl+X**: Cut selected text
- **Ctrl+Z**: Undo (in text areas)

## 🔧 Configuration Options

### API Keys
- **Gemini API Key**: Required for both text processing and Gemini TTS
- **Google Cloud Service Account**: Optional, only needed for Google Cloud TTS

### Settings Persistence
The application automatically saves:
- Service and model selections
- AI enhancement preferences
- Voice and audio settings
- API configurations
- Output preferences

### Output Configuration
- **Custom Filenames**: Specify output file names
- **Format Support**: 
  - Gemini TTS: WAV files
  - Google Cloud TTS: MP3 and WAV files
- **Quality Settings**: Automatic optimization for each service

## 🛠️ Troubleshooting

### Common Issues

1. **"google-genai library not found"**:
   - The app will automatically try to install it
   - Or manually: `pip install google-genai`

2. **Gemini TTS API Errors**:
   - Ensure you have a valid Gemini API key
   - Check that the key has access to TTS preview features
   - Verify internet connection

3. **Audio Playback Issues**:
   - Make sure pygame is properly installed
   - Check system audio settings
   - Try different audio formats

4. **Google Cloud TTS Setup**:
   - Service account JSON file required
   - Ensure Text-to-Speech API is enabled
   - Check billing account status

### Error Reporting
The application provides detailed error messages in the status section and terminal output for debugging.

## 🔐 Security Notes

- **API Key Protection**: Never commit API keys to version control
- **Production Use**: Use environment variables or secret managers
- **Service Accounts**: Store JSON files securely, outside of project directories

## 🎨 Customization

The application is built with modularity in mind:
- **UI Themes**: Easily modify CustomTkinter appearance
- **Voice Options**: Extend voice libraries and options
- **Model Support**: Add new Gemini models as they become available
- **Output Formats**: Extend audio format support

## 📞 Support

- **Issues**: Report bugs or feature requests
- **Documentation**: Comprehensive field descriptions in-app
- **Updates**: Regular updates for new Gemini features

## 📄 License

This project is open-source. Please check individual dependencies for their respective licenses.

---

**Latest Update**: Fixed Gemini Native TTS implementation using the new Google GenAI SDK with proper Live API integration for audio generation. 