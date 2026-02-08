# راهنمای امنیتی API Keys

این راهنما به شما کمک می‌کند تا از leaked شدن API key های خود جلوگیری کنید.

## ⚠️ چرا API Key ها Leaked می‌شوند؟

API key ها معمولاً به دلایل زیر leaked می‌شوند:
1. **آپلود به GitHub/GitLab**: اگر فایل CSV حاوی API key را به repository عمومی push کنید
2. **اشتراک‌گذاری فایل**: ارسال فایل CSV به دیگران یا آپلود در سرویس‌های عمومی
3. **لاگ‌های عمومی**: نمایش API key در لاگ‌ها، console، یا error messages
4. **کدهای عمومی**: hardcode کردن API key در کد و آپلود آن
5. **فایل‌های backup**: آپلود فایل‌های backup حاوی API key

## 🛡️ راه‌های جلوگیری از Leaked شدن

### 1. استفاده از .gitignore

**مهم**: همیشه فایل CSV حاوی API key ها را در `.gitignore` قرار دهید.

```bash
# در فایل .gitignore اضافه کنید:
*.csv
api_keys*.csv
*_keys.csv
secrets/
.env
```

### 2. محدودیت‌های API Key در Google Cloud Console

1. به [Google AI Studio](https://aistudio.google.com/apikey) بروید
2. روی API key خود کلیک کنید
3. **Application restrictions** را فعال کنید:
   - **IP addresses**: فقط IP های مجاز را اضافه کنید
   - **HTTP referrers**: اگر از وب استفاده می‌کنید
4. **API restrictions** را تنظیم کنید:
   - فقط API های مورد نیاز را فعال کنید (مثلاً فقط Gemini API)

### 3. استفاده از Environment Variables (اختیاری)

به جای فایل CSV، می‌توانید از environment variables استفاده کنید:

```bash
# در terminal:
export GEMINI_API_KEY_1="your-api-key-1"
export GEMINI_API_KEY_2="your-api-key-2"
```

### 4. محافظت از فایل CSV

- فایل CSV را در مسیر امن نگه دارید (مثلاً خارج از پروژه)
- از permission های مناسب استفاده کنید:
  ```bash
  chmod 600 api_keys.csv  # فقط owner می‌تواند بخواند/بنویسد
  ```
- هرگز فایل CSV را به کسی ارسال نکنید
- از password manager برای ذخیره API key ها استفاده کنید

### 5. بررسی لاگ‌ها

- همیشه لاگ‌ها را بررسی کنید تا مطمئن شوید API key در آن‌ها نیست
- فایل `content_automation.log` را در `.gitignore` قرار دهید
- قبل از اشتراک‌گذاری لاگ‌ها، آن‌ها را بررسی کنید

### 6. استفاده از API Key Rotation

- از چندین API key استفاده کنید (rotation)
- اگر یک key leaked شد، فوراً آن را حذف کنید
- همیشه یک backup key داشته باشید

### 7. محدودیت Rate Limiting

- از rate limiting استفاده کنید تا از abuse جلوگیری شود
- اگر متوجه استفاده غیرعادی شدید، فوراً key را revoke کنید

## 🔍 چک‌لیست امنیتی

قبل از push کردن کد به Git:

- [ ] فایل CSV API keys در `.gitignore` است
- [ ] هیچ API key در کد hardcode نشده
- [ ] لاگ‌ها بررسی شده و API key در آن‌ها نیست
- [ ] فایل‌های backup حاوی API key نیستند
- [ ] API key restrictions در Google Cloud Console تنظیم شده
- [ ] فقط API های مورد نیاز فعال هستند

## 🚨 اگر API Key شما Leaked شد

1. **فوراً API key را revoke کنید**:
   - به [Google AI Studio](https://aistudio.google.com/apikey) بروید
   - API key leaked شده را حذف کنید

2. **یک API key جدید بسازید**:
   - API key جدید از Google AI Studio بگیرید
   - محدودیت‌های امنیتی را تنظیم کنید

3. **فایل CSV را به‌روزرسانی کنید**:
   - API key قدیمی را حذف کنید
   - API key جدید را اضافه کنید

4. **بررسی کنید که کجا leaked شده**:
   - GitHub/GitLab history را بررسی کنید
   - لاگ‌های عمومی را بررسی کنید
   - فایل‌های backup را بررسی کنید

## 📝 بهترین روش‌ها (Best Practices)

1. **هرگز API key را در کد hardcode نکنید**
2. **همیشه از فایل‌های خارجی (CSV) یا environment variables استفاده کنید**
3. **فایل CSV را در `.gitignore` قرار دهید**
4. **از API key restrictions استفاده کنید**
5. **لاگ‌ها را به صورت منظم بررسی کنید**
6. **از password manager برای مدیریت API key ها استفاده کنید**
7. **API key ها را به صورت منظم rotate کنید**
8. **از چندین API key استفاده کنید (rotation)**
9. **فایل CSV را با permission های مناسب محافظت کنید**
10. **هرگز فایل CSV را به کسی ارسال نکنید**

## 🔗 لینک‌های مفید

- [Google AI Studio - API Keys](https://aistudio.google.com/apikey)
- [Google Cloud Console - API Credentials](https://console.cloud.google.com/apis/credentials)
- [API Key Security Best Practices](https://cloud.google.com/docs/authentication/api-keys)

## ⚡ نکات سریع

- ✅ فایل CSV را در `.gitignore` قرار دهید
- ✅ API key restrictions را در Google Cloud Console تنظیم کنید
- ✅ از چندین API key استفاده کنید
- ✅ لاگ‌ها را بررسی کنید
- ❌ هرگز API key را در کد hardcode نکنید
- ❌ فایل CSV را به repository push نکنید
- ❌ فایل CSV را به کسی ارسال نکنید





























