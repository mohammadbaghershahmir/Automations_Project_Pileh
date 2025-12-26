#!/bin/bash

# Script to push changes to GitHub repository
# Make sure git is installed: sudo apt install git

set -e  # Exit on error

cd /media/shahmir/Program/automation_pileh

# Check if git is installed
if ! command -v git &> /dev/null; then
    echo "Error: git is not installed. Please install it first:"
    echo "  sudo apt install git"
    exit 1
fi

# Initialize git repository if not already initialized
if [ ! -d .git ]; then
    echo "Initializing git repository..."
    git init
    git branch -M main
fi

# Check if remote exists
if ! git remote | grep -q origin; then
    echo "Adding remote repository..."
    git remote add origin https://github.com/mohammadbaghershahmir/Automations_Project_Pileh.git
else
    echo "Updating remote URL..."
    git remote set-url origin https://github.com/mohammadbaghershahmir/Automations_Project_Pileh.git
fi

# Create .gitignore if it doesn't exist
if [ ! -f .gitignore ]; then
    echo "Creating .gitignore file..."
    cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
build/
dist/
*.egg-info/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Logs
*.log

# OS
.DS_Store
Thumbs.db

# Project specific
*.mp3
*.wav
*.json
!prompts.json
*.csv
*.xlsx
*.xls
*.pdf
*.docx
*.txt
!requirements*.txt
!README*.md
!*.spec
EOF
fi

# Add all files
echo "Adding all files..."
git add .

# Check if there are changes to commit
if git diff --staged --quiet; then
    echo "No changes to commit."
else
    # Commit changes
    echo "Committing changes..."
    git commit -m "Clean up codebase: Remove old Stage 3 and Stage 4 forms

- Removed old separate Stage 3 and Stage 4 form windows
- Removed process_third_stage_worker, process_fourth_stage_worker methods
- Removed show_third_stage_window, show_fourth_stage_window methods
- Removed orphaned code blocks
- Added process_pdf method for initial PDF processing
- Added view_csv_from_json method for CSV viewing
- Added main() function for run.py compatibility
- Codebase is now cleaner and uses only the new multi-part processing system"
    
    # Push to GitHub
    echo "Pushing to GitHub..."
    git push -u origin main
    
    echo "✅ Successfully pushed to GitHub!"
fi



