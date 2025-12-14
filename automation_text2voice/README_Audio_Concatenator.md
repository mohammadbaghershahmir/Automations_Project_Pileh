# Audio File Concatenator

A simple GUI application for concatenating multiple audio files into a single MP3 file with customizable quality settings.

## Features

- **Multi-format Support**: Supports WAV, MP3, M4A, AAC, OGG, and FLAC audio files
- **Drag & Drop Interface**: Easy-to-use GUI with file management
- **Quality Options**: Choose from 4 different MP3 quality levels (64-320 kbps)
- **File Reordering**: Move files up/down to control concatenation order
- **Progress Tracking**: Real-time progress bar and status updates
- **Batch Processing**: Process multiple files at once

## Requirements

### System Requirements
- Python 3.7 or higher
- FFmpeg (required for audio processing)

### Python Dependencies
- pydub
- tkinter (usually included with Python)

## Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements_audio_concatenator.txt
   ```

2. **Install FFmpeg:**
   
   **Windows:**
   - Download FFmpeg from https://ffmpeg.org/download.html
   - Extract and add the `bin` folder to your system PATH
   - Or use chocolatey: `choco install ffmpeg`
   
   **macOS:**
   ```bash
   brew install ffmpeg
   ```
   
   **Linux (Ubuntu/Debian):**
   ```bash
   sudo apt update
   sudo apt install ffmpeg
   ```

## Usage

1. **Run the application:**
   ```bash
   python audio_concatenator.py
   ```

2. **Add audio files:**
   - Click "Add Files" to select multiple audio files
   - Supported formats: WAV, MP3, M4A, AAC, OGG, FLAC

3. **Organize files (optional):**
   - Select files and use "Move Up"/"Move Down" to reorder
   - Use "Remove Selected" to remove unwanted files
   - Use "Clear All" to start over

4. **Choose output settings:**
   - Select desired MP3 quality from dropdown
   - Click "Browse" to choose output file location

5. **Process files:**
   - Click "Concatenate Audio Files" to start processing
   - Monitor progress in the progress bar
   - Wait for completion message

## Quality Options

| Quality Level | Bitrate | File Size | Use Case |
|---------------|---------|-----------|----------|
| Low | 64 kbps | Smallest | Voice recordings, podcasts |
| Medium | 128 kbps | Small | General use, streaming |
| High | 192 kbps | Medium | Good quality music |
| Very High | 320 kbps | Largest | High-quality music, archival |

## Features Explained

### File Management
- **Add Files**: Select multiple audio files at once
- **Remove Selected**: Remove specific files from the list
- **Clear All**: Remove all files from the list
- **Move Up/Down**: Change the order of concatenation

### Processing
- Files are processed in the order they appear in the list
- The application automatically detects audio formats
- Progress is shown in real-time
- Error handling for corrupted or unsupported files

### Output
- Always outputs as MP3 format
- Customizable bitrate/quality
- Preserves audio length and content
- Adds basic metadata tags

## Troubleshooting

### Common Issues

1. **"FFmpeg is required" error:**
   - Install FFmpeg and ensure it's in your system PATH
   - Restart the application after installation

2. **"Error processing file" messages:**
   - Check if the audio file is corrupted
   - Ensure the file format is supported
   - Try converting the file to WAV or MP3 first

3. **Out of memory errors:**
   - Process fewer files at once
   - Use lower quality settings
   - Ensure sufficient disk space

4. **Slow processing:**
   - Large files take more time
   - Higher quality settings require more processing
   - Close other applications to free up resources

### Performance Tips

- Use WAV files for fastest processing
- Lower quality settings process faster
- Keep individual files under 100MB for best performance
- Ensure sufficient free disk space (2x the total input size)

## Technical Details

### Supported Formats
- **Input**: WAV, MP3, M4A, AAC, OGG, FLAC
- **Output**: MP3 only

### Dependencies
- **pydub**: Audio processing and format conversion
- **tkinter**: GUI framework (built into Python)
- **FFmpeg**: Audio codec support (external dependency)

### Processing Flow
1. Load each audio file using pydub
2. Concatenate files in sequence
3. Export combined audio as MP3
4. Apply selected quality/bitrate settings

## License

This project is open source and available under the MIT License.

## Support

For issues or questions:
1. Check the troubleshooting section
2. Ensure all dependencies are properly installed
3. Verify FFmpeg installation with: `ffmpeg -version`

