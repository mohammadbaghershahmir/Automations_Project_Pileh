# دستورالعمل ساخت فایل Executable

## روش 1: استفاده از اسکریپت خودکار (پیشنهادی)

```bash
cd content_automation_project
./build_exe.sh
```

## روش 2: استفاده از PyInstaller به صورت دستی

### نصب PyInstaller

```bash
pip install pyinstaller
```

### ساخت Executable

```bash
pyinstaller build_exe.spec --clean --noconfirm
```

فایل executable در پوشه `dist/` ساخته می‌شود.

## نکات مهم:

### برای ویندوز:
- برای ساخت exe برای ویندوز، باید روی سیستم ویندوز build کنید
- یا از Wine استفاده کنید (اما ممکن است مشکلاتی داشته باشد)
- یا از یک ماشین مجازی ویندوز استفاده کنید

### برای لینوکس:
- فایل executable برای لینوکس ساخته می‌شود
- می‌توانید آن را به هر سیستم لینوکسی منتقل کنید

### برای macOS:
- برای ساخت app برای macOS، باید روی macOS build کنید

## ساختار فایل‌ها:

- `build_exe.spec`: فایل تنظیمات PyInstaller
- `build_exe.sh`: اسکریپت خودکار برای build
- `dist/`: پوشه خروجی executable

## وابستگی‌ها:

تمام وابستگی‌ها به صورت خودکار در executable قرار می‌گیرند:
- customtkinter
- google-generativeai
- PyPDF2
- python-docx
- و سایر کتابخانه‌ها

## استفاده از Executable:

1. فایل executable را از پوشه `dist/` کپی کنید
2. فایل `prompts.json` را در همان پوشه قرار دهید (اگر نیاز دارید)
3. فایل executable را اجرا کنید

## عیب‌یابی:

اگر executable کار نکرد:
1. لاگ‌ها را بررسی کنید
2. مطمئن شوید که تمام وابستگی‌ها نصب شده‌اند
3. از `--debug=all` در PyInstaller استفاده کنید





