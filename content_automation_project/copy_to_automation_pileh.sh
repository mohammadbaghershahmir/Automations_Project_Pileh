#!/bin/bash
# کپی خودکار از zih به automation_pileh
# این اسکریپت را از داخل پوشه zih اجرا کن

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# مسیر مقصد (automation_pileh) - اگر اینجا نیست، از کاربر بپرس
DEST="/media/shahmir/Program/automation_pileh/content_automation_project"

if [ ! -d "$DEST" ]; then
    echo "⚠ پوشه پیش‌فرض پیدا نشد: $DEST"
    echo "لطفاً مسیر صحیح automation_pileh را وارد کن:"
    read -p "مسیر automation_pileh: " DEST
    if [ ! -d "$DEST" ]; then
        echo "❌ پوشه پیدا نشد: $DEST"
        exit 1
    fi
fi

echo "📁 از: $SCRIPT_DIR"
echo "📁 به: $DEST"
echo ""

FILES=(
    "main_gui.py"
    "reference_change_csv_processor.py"
    "api_layer.py"
    "stage_settings_manager.py"
    "unified_api_client.py"
    "run.py"
)

SUCCESS=0
for file in "${FILES[@]}"; do
    if [ -f "$SCRIPT_DIR/$file" ]; then
        cp "$SCRIPT_DIR/$file" "$DEST/$file" && echo "✓ $file" && ((SUCCESS++))
    else
        echo "⚠ $file پیدا نشد"
    fi
done

echo ""
echo "✅ $SUCCESS فایل کپی شد."
echo "حالا برنامه را از $DEST اجرا کن."
