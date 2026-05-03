"""
Main entry point for Content Automation Project.
Runs with cwd = this file's directory (project root) so settings and paths are consistent.
"""

import sys
import os
from dotenv import load_dotenv

_project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _project_root)
if os.getcwd() != _project_root:
    os.chdir(_project_root)
load_dotenv(os.path.join(_project_root, ".env"))

from main_gui import main

if __name__ == "__main__":
    main()
































