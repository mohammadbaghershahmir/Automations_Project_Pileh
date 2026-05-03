---
name: OpenRouter Env Migration
overview: مهاجرت سراسری پروژه از منطق CSV key-rotation به الگوی تک‌کلیدی OpenRouter با خواندن API key از `.env`، همراه با حذف مسیرهای Google/DeepSeek در روتینگ و UI.
todos:
  - id: env-config
    content: افزودن بارگذاری `.env` و قرارداد متغیرهای OPENROUTER_*
    status: completed
  - id: key-manager-refactor
    content: بازنویسی APIKeyManager برای single key و حذف وابستگی CSV
    status: completed
  - id: unified-routing-openrouter
    content: یکسان‌سازی روتینگ همه stageها روی OpenRouter در UnifiedAPIClient
    status: completed
  - id: gui-api-section-cleanup
    content: حذف UI مربوط به CSV key files و جایگزینی با وضعیت `.env`
    status: completed
  - id: client-docs-tests
    content: به‌روزرسانی OpenRouter client، README، requirements و اجرای smoke test
    status: completed
isProject: false
---

# بازنویسی منطق API به OpenRouter + .env

## هدف
کل پروژه به‌جای بارگذاری API key از CSV، فقط از OpenRouter استفاده کند و کلید از `.env` خوانده شود (single key).

## فایل‌های اصلی درگیر
- [main_gui.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/main_gui.py)
- [unified_api_client.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/unified_api_client.py)
- [api_layer.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/api_layer.py)
- [openrouter_api_client.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/openrouter_api_client.py)
- [stage_settings_manager.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/stage_settings_manager.py)
- [requirements.txt](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/requirements.txt)
- [README.md](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/README.md)

## طرح اجرا
1. **اضافه‌کردن لایه تنظیمات محیطی**
   - افزودن بارگذاری `.env` (مثلاً با `python-dotenv`) در startup.
   - تعریف قرارداد ثابت: `OPENROUTER_API_KEY` و (اختیاری) `OPENROUTER_MODEL`.
   - حذف وابستگی به `load_from_csv` در جریان اجرای اصلی.

2. **ساده‌سازی مدیریت کلیدها به single-key**
   - بازطراحی `APIKeyManager` در [api_layer.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/api_layer.py) برای حالت single key از env.
   - حذف/غیرفعال‌سازی مسیرهای rotation، account/project، و پیام‌های مرتبط با CSV.
   - نگه‌داشتن `sanitize_error_message` برای جلوگیری از نشت کلید در لاگ.

3. **یکپارچه‌سازی روتینگ API روی OpenRouter**
   - در [unified_api_client.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/unified_api_client.py) تمام stageها به OpenRouter route شوند.
   - حذف fallbackهای Google/DeepSeek و کاهش پیچیدگی branchها.
   - پیش‌فرض مدل از `OPENROUTER_MODEL` یا `APIConfig.DEFAULT_OPENROUTER_MODEL` خوانده شود.

4. **پاکسازی/بازطراحی UI API Configuration**
   - در [main_gui.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/main_gui.py):
     - حذف 3 ورودی CSV و دکمه‌های Browse مربوطه.
     - نمایش وضعیت خواندن `.env` (مثلاً “OpenRouter key loaded from .env”).
     - جایگزینی validationهای «Please load ... API keys» با پیام «کلید در `.env` موجود نیست».
   - حفظ UX فعلی stage settings تا جایی که با provider ثابت OpenRouter سازگار است.

5. **پایدارسازی کلاینت OpenRouter برای همه stageها**
   - در [openrouter_api_client.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/openrouter_api_client.py) دریافت کلید از env به‌صورت مرکزی و قابل اتکا.
   - اطمینان از رفتار یکسان در `process_text`، `process_pdf_with_prompt` و batch.

6. **حذف مسیرهای قدیمی و به‌روزرسانی مستندات**
   - حذف/بی‌اثرکردن مسیرهای وابسته به Google/DeepSeek/CSV که دیگر استفاده نمی‌شوند.
   - در [requirements.txt](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/requirements.txt) افزودن `python-dotenv`.
   - در [README.md](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/README.md) جایگزینی راهنمای CSV با راهنمای `.env`:
     - `OPENROUTER_API_KEY=...`
     - `OPENROUTER_MODEL=...` (اختیاری)

7. **اعتبارسنجی نهایی**
   - تست smoke برای مسیرهای اصلی (حداقل: یک پردازش متنی و یک پردازش PDF).
   - بررسی اینکه هیچ نقطه‌ای از UI/کد به فایل‌های `api_keys.csv`، `deepseek_api_keys.csv`، `openrouter_api_keys.csv` وابسته نمانده باشد.

## نکات ریسک و کنترل
- [main_gui.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/main_gui.py) بسیار بزرگ است؛ تغییرات API باید حداقلی ولی سیستماتیک انجام شود تا regressions کم شود.
- برخی stageها قبلاً رفتار متفاوت provider داشتند؛ با provider ثابت OpenRouter لازم است default modelها برای هر stage بررسی شوند.
- پیام‌های خطا و راهنمای کاربر باید کاملاً از CSV به `.env` مهاجرت داده شوند تا سردرگمی ایجاد نشود.