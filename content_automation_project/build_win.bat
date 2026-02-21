@echo off
REM Build Content Automation Project - Windows executable
REM Run this script on Windows (double-click or: build_win.bat)

echo ==========================================
echo Building Content Automation for Windows
echo ==========================================

REM Change to script directory (content_automation_project)
cd /d "%~dp0"

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python not found. Please install Python and add it to PATH.
    pause
    exit /b 1
)

REM Install PyInstaller if needed
pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo Installing PyInstaller...
    pip install pyinstaller
)

REM Install project dependencies if not already installed (optional)
REM Run: pip install -r requirements.txt

REM Clean previous build
echo Cleaning previous build...
if exist "build" rmdir /s /q build
if exist "dist" rmdir /s /q dist

REM Build
echo Building executable (this may take a few minutes)...
pyinstaller build_exe.spec --clean --noconfirm

if exist "dist\ContentAutomation.exe" (
    echo.
    echo ==========================================
    echo Build successful!
    echo ==========================================
    echo Output: dist\ContentAutomation.exe
    echo.
    echo You can copy ContentAutomation.exe to any Windows PC and run it.
    echo No Python installation required on the target PC.
    echo.
) else (
    echo.
    echo ==========================================
    echo Build failed. Check the messages above.
    echo ==========================================
)

pause
