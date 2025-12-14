import os
import subprocess
import sys
from pydub.utils import which

def check_ffmpeg_installation():
    print("=== FFmpeg Installation Diagnostic ===\n")
    
    # Check 1: pydub's which function
    print("1. Checking with pydub.utils.which:")
    ffmpeg_path = which("ffmpeg")
    if ffmpeg_path:
        print(f"   ✓ Found: {ffmpeg_path}")
    else:
        print("   ✗ Not found by pydub")
    
    # Check 2: System PATH
    print("\n2. Checking system PATH:")
    path_dirs = os.environ.get('PATH', '').split(os.pathsep)
    ffmpeg_found_in_path = False
    for path_dir in path_dirs:
        if path_dir and os.path.exists(path_dir):
            ffmpeg_exe = os.path.join(path_dir, 'ffmpeg.exe')
            if os.path.exists(ffmpeg_exe):
                print(f"   ✓ Found in PATH: {ffmpeg_exe}")
                ffmpeg_found_in_path = True
                break
    
    if not ffmpeg_found_in_path:
        print("   ✗ ffmpeg.exe not found in any PATH directory")
    
    # Check 3: Common installation locations
    print("\n3. Checking common installation locations:")
    common_paths = [
        r"C:\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files (x86)\ffmpeg\bin\ffmpeg.exe",
        r"C:\ProgramData\chocolatey\lib\ffmpeg\tools\ffmpeg\bin\ffmpeg.exe"
    ]
    
    for path in common_paths:
        if os.path.exists(path):
            print(f"   ✓ Found: {path}")
        else:
            print(f"   ✗ Not found: {path}")
    
    # Check 4: Try running ffmpeg directly
    print("\n4. Testing ffmpeg execution:")
    try:
        result = subprocess.run(['ffmpeg', '-version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("   ✓ ffmpeg runs successfully")
            print(f"   Version info: {result.stdout.split()[2] if len(result.stdout.split()) > 2 else 'Unknown'}")
        else:
            print(f"   ✗ ffmpeg failed with return code: {result.returncode}")
    except FileNotFoundError:
        print("   ✗ ffmpeg command not found")
    except subprocess.TimeoutExpired:
        print("   ✗ ffmpeg command timed out")
    except Exception as e:
        print(f"   ✗ Error running ffmpeg: {e}")
    
    # Check 5: Current working directory
    print(f"\n5. Current working directory: {os.getcwd()}")
    
    # Check 6: Python version and platform
    print(f"\n6. Python version: {sys.version}")
    print(f"   Platform: {sys.platform}")
    
    print("\n=== Recommendations ===")
    if not ffmpeg_path:
        print("FFmpeg is not properly installed or not in PATH.")
        print("\nTo fix this:")
        print("1. Download FFmpeg from: https://www.gyan.dev/ffmpeg/builds/")
        print("2. Extract to C:\\ffmpeg\\")
        print("3. Add C:\\ffmpeg\\bin to your system PATH")
        print("4. Restart your terminal/IDE")
        print("5. Run this diagnostic again")
    else:
        print("FFmpeg appears to be properly installed!")

if __name__ == "__main__":
    check_ffmpeg_installation()

