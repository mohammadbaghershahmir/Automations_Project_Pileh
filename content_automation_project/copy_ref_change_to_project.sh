#!/bin/bash
# Copy updated Reference Change + OpenRouter files to the folder where you run the app.
# Includes: OpenRouter (z-ai/glm-5), full extraction (paragraphs+tables+figs), JSON upload.
#
# Usage: pass the folder that contains main_gui.py (your project folder).
# Example:
#   ./copy_ref_change_to_project.sh /media/shahmir/Program/automation_pileh/content_automation_project

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "$1" ]; then
  echo "Usage: $0 <destination_folder>"
  echo "  Example: $0 /media/shahmir/Program/automation_pileh/content_automation_project"
  echo "  (Use the folder where you run python main_gui.py)"
  exit 1
fi

DEST="$1"
if [ ! -d "$DEST" ]; then
  echo "Error: Not a directory: $DEST"
  exit 1
fi

cp "$SCRIPT_DIR/main_gui.py" "$DEST/main_gui.py" && echo "Copied main_gui.py -> $DEST"
cp "$SCRIPT_DIR/reference_change_csv_processor.py" "$DEST/reference_change_csv_processor.py" && echo "Copied reference_change_csv_processor.py -> $DEST"
cp "$SCRIPT_DIR/api_layer.py" "$DEST/api_layer.py" && echo "Copied api_layer.py -> $DEST"
cp "$SCRIPT_DIR/stage_settings_manager.py" "$DEST/stage_settings_manager.py" && echo "Copied stage_settings_manager.py -> $DEST"
cp "$SCRIPT_DIR/unified_api_client.py" "$DEST/unified_api_client.py" && echo "Copied unified_api_client.py -> $DEST"
cp "$SCRIPT_DIR/run.py" "$DEST/run.py" && echo "Copied run.py -> $DEST"
echo ""
echo "✅ Done. Run your app from $DEST to use OpenRouter (z-ai/glm-5) and new Reference Change."
