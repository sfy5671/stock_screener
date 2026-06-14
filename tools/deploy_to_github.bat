@echo off
chcp 65001 >nul
title 飆股篩選器 - 上傳到 GitHub

echo ============================================================
echo   飆股篩選器 - 自動上傳到 GitHub
echo ============================================================
echo.

REM 切換到專案目錄
cd /d "D:\python程式開發\飆股篩選器"

REM 詢問 GitHub 帳號
set /p GITHUB_USER=請輸入你的 GitHub 帳號名稱:
if "%GITHUB_USER%"=="" (
    echo 錯誤：帳號不能空白！
    pause
    exit /b
)

echo.
echo [1/5] 初始化 Git 倉庫...
if not exist ".git" (
    git init
    echo      Git 初始化完成
) else (
    echo      Git 已經初始化過了
)

echo.
echo [2/5] 加入所有檔案...
git add app.py stock_screener.py requirements.txt gunicorn_config.py render.yaml Procfile .gitignore
git add templates/ static/
git add DEPLOY.md
git status --short

echo.
echo [3/5] 建立 commit...
git commit -m "飆股篩選器 v3.0 - 台股技術分析選股系統"

echo.
echo [4/5] 連結 GitHub 倉庫...
echo      倉庫: https://github.com/%GITHUB_USER%/stock-screener
echo.
echo *** 重要：請先到 GitHub 網站建立倉庫 ***
echo    1. 開啟 https://github.com/new
echo    2. Repository name 輸入: stock-screener
echo    3. 選 Private (私人)
echo    4. 不要勾選 Add README
echo    5. 點 Create repository
echo.
pause

git remote remove origin 2>nul
git remote add origin https://github.com/%GITHUB_USER%/stock-screener.git
git branch -M main

echo.
echo [5/5] 上傳到 GitHub...
echo      (會跳出登入視窗，請用瀏覽器登入 GitHub 授權)
echo.
git push -u origin main

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================================
    echo   上傳成功！
    echo ============================================================
    echo.
    echo   你的倉庫: https://github.com/%GITHUB_USER%/stock-screener
    echo.
    echo   接下來請到 Render 部署:
    echo   1. 開啟 https://render.com
    echo   2. 用 GitHub 帳號登入
    echo   3. New + → Web Service
    echo   4. 選擇 stock-screener 倉庫
    echo   5. Region 選 Singapore
    echo   6. Build Command: pip install -r requirements.txt
    echo   7. Start Command: gunicorn -c gunicorn_config.py app:app
    echo   8. 選 Free 或 Starter ($7/月)
    echo   9. 點 Create Web Service
    echo   10. 等 3~5 分鐘部署完成！
    echo.
    echo ============================================================
) else (
    echo.
    echo 上傳失敗！請確認：
    echo   1. GitHub 帳號名稱是否正確
    echo   2. 倉庫 stock-screener 是否已建立
    echo   3. 網路是否正常
    echo.
)

pause
