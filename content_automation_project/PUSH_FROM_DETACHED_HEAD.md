# Push از حالت Detached HEAD

وقتی روی هیچ branch نیستید (detached HEAD)، باید branch مقصد را صریح بگویید:

```bash
git push origin HEAD:main
```

یعنی: «وضعیت فعلی HEAD را به شاخهٔ `main` روی `origin` push کن.»

اگر branch دیگری مدنظر است (مثلاً `develop`):

```bash
git push origin HEAD:develop
```

اگر می‌خواهید بعداً روی یک branch کار کنید، می‌توانید از همین commit یک branch بسازید و بعد push کنید:

```bash
git checkout -b my-branch    # ساخت branch از commit فعلی
git push origin my-branch   # push آن branch
```
