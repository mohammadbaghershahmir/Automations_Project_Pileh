#!/bin/bash
# Build executable script for Content Automation Project

echo "=========================================="
echo "Building Content Automation Executable"
echo "=========================================="

# Check if PyInstaller is installed
if ! command -v pyinstaller &> /dev/null; then
    echo "PyInstaller not found. Installing..."
    pip install pyinstaller
fi

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf build dist __pycache__ *.spec

# Build executable
echo "Building executable..."
pyinstaller build_exe.spec --clean --noconfirm

# Check if build was successful
if [ -f "dist/ContentAutomation" ] || [ -f "dist/ContentAutomation.exe" ]; then
    echo ""
    echo "=========================================="
    echo "✓ Build successful!"
    echo "=========================================="
    echo "Executable location: dist/ContentAutomation (or ContentAutomation.exe on Windows)"
    echo ""
    echo "Note: For Windows executable, you need to build on Windows or use Wine."
    echo "On Linux, this will create a Linux executable."
else
    echo ""
    echo "=========================================="
    echo "✗ Build failed!"
    echo "=========================================="
    echo "Check the error messages above."
fi


