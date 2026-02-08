# دستورالعمل ساخت فایل نصبی برای ویندوز

## خروجی نهایی

بعد از build، یک فایل **`ContentAutomation.exe`** در پوشه `dist/` ساخته می‌شود. این فایل را می‌توانید به هر ویندوزی کپی کنید و بدون نصب Python اجرا کنید (فایل نصبی تک‌فایل).

---

## روش ۱: ساخت روی ویندوز (پیشنهادی)

### پیش‌نیاز

- نصب **Python 3.10 یا 3.11** روی ویندوز
- نصب وابستگی‌های پروژه:

```cmd
cd content_automation_project
pip install -r requirements.txt
pip install pyinstaller
```

### ساخت exe

**گزینه الف – دو بار کلیک روی اسکریپت:**

1. فایل **`build_win.bat`** را با دوبار کلیک اجرا کنید.
2. بعد از چند دقیقه، در پوشه **`dist\`** فایل **`ContentAutomation.exe`** ساخته می‌شود.

**گزینه ب – از خط فرمان:**

```cmd
cd content_automation_project
build_win.bat
```

یا به‌صورت دستی:

```cmd
cd content_automation_project
pyinstaller build_exe.spec --clean --noconfirm
```

خروجی: **`dist\ContentAutomation.exe`**

---

## روش ۲: ساخت از لینوکس/مک برای ویندوز

برای ساخت exe مخصوص ویندوز باید یا روی **خود ویندوز** build کنید، یا از **Wine** یا **ماشین مجازی ویندوز** استفاده کنید. روی لینوکس با اسکریپت زیر فقط اجرایی لینوکس ساخته می‌شود:

```bash
cd content_automation_project
./build_exe.sh
```

---

## استفاده از فایل نصبی

1. فایل **`ContentAutomation.exe`** را از پوشه **`dist/`** کپی کنید.
2. **`prompts.json`** داخل همان exe باندل شده؛ برای اجرای عادی نیازی به کپی کردن آن نیست.
3. در ویندوز هدف، فقط **`ContentAutomation.exe`** را اجرا کنید (بدون نصب Python).

اگر بخواهید پرامپت‌ها را عوض کنید، می‌توانید یک **`prompts.json`** در همان پوشهٔ exe قرار دهید؛ در صورت وجود، برنامه از آن استفاده می‌کند (در صورت پشتیبانی در کد). در حالت پیش‌فرض همان پرامپت‌های داخل exe استفاده می‌شوند.

---

## ساختار فایل‌ها

| فایل / پوشه       | توضیح |
|-------------------|--------|
| `build_exe.spec`  | تنظیمات PyInstaller برای ساخت exe |
| `build_win.bat`   | اسکریپت ساخت برای ویندوز (دوبار کلیک یا از CMD) |
| `build_exe.sh`    | اسکریپت ساخت برای لینوکس/مک |
| `dist/`           | خروجی: بعد از build حاوی `ContentAutomation.exe` |
| `build/`          | فایل‌های موقت build؛ بعد از build قابل حذف است |

---

## وابستگی‌های باندل‌شده

این موارد داخل exe قرار می‌گیرند و روی ویندوز هدف نیازی به نصب جداگانه ندارند:

- customtkinter, Pillow
- google-generativeai, requests
- PyMuPDF (fitz), PyPDF2
- python-docx
- sentence-transformers, numpy (برای RAG)
- و ماژول‌های دیگر پروژه

---

## عیب‌یابی

- **exe اجرا نمی‌شود:**  
  - از **خط فرمان** اجرا کنید تا پیام خطا را ببینید:  
    `ContentAutomation.exe`  
  - مطمئن شوید Visual C++ Redistributable (در صورت نیاز) نصب است.

- **خطای وابستگی هنگام build:**  
  ```cmd
  pip install -r requirements.txt
  pip install pyinstaller
  ```
  سپس دوباره `build_win.bat` یا دستور `pyinstaller build_exe.spec --clean --noconfirm` را اجرا کنید.

- **ساخت نصب‌کننده (Installer) اختیاری:**  
  می‌توانید با **Inno Setup** یا **NSIS** از پوشه `dist/` یک فایل Setup.exe برای نصب برنامه بسازید.
