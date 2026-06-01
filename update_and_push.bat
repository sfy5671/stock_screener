@echo off
chcp 65001 >nul
title 飆股篩選器 - 更新上傳

cd /d "D:\python程式開發\飆股篩選器"

echo ============================================================
echo   飆股篩選器 - 更新上傳到雲端
echo ============================================================
echo.

echo [1/3] 檢查變更...
git status --short
echo.

set /p MSG=請輸入更新說明 (直接按Enter用預設):
if "%MSG%"=="" set MSG=更新飆股篩選器

echo.
echo [2/3] 提交變更...
git add app.py stock_screener.py templates/ static/ requirements.txt gunicorn_config.py render.yaml Procfile .gitignore
git commit -m "%MSG%"

echo.
echo [3/3] 上傳到 GitHub (Render 會自動重新部署)...
git push

echo.
echo ============================================================
echo   上傳完成！Render 會在 2~3 分鐘內自動更新
echo   網址: https://stock-screener-beza.onrender.com
echo ============================================================
echo.

pause
