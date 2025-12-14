"""
Main entry point for Content Automation Project
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main_gui import main

if __name__ == "__main__":
    main()


