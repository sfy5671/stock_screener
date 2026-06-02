@echo off
chcp 65001 >nul
REM ============================================================
REM  飆股篩選器 - 每日資料更新 + 自動 push GitHub
REM ============================================================
REM  建議排程：台股盤後（14:30 之後）每天執行一次
REM  Windows 工作排程器 → 新增工作 → 觸發程序「每日 15:00」→ 動作「啟動程式」此 .bat
REM ============================================================

cd /d "%~dp0"

echo.
echo === Step 1/3: 跑 daily_refresh.py 抓最新資料 ===
echo.
python daily_refresh.py
if errorlevel 1 (
    echo.
    echo ✗ 抓取失敗，取消 push
    pause
    exit /b 1
)

echo.
echo === Step 2/3: git add + commit ===
echo.
git add .cache/basic_indicators.json .cache/company_capital.json .cache/inst_hist_5.json .cache/margin.json

REM 用日期當 commit 訊息
for /f "tokens=2 delims==" %%I in ('"wmic os get localdatetime /value"') do set DT=%%I
set TODAY=%DT:~0,4%-%DT:~4,2%-%DT:~6,2%

git commit -m "daily data refresh %TODAY%"
if errorlevel 1 (
    echo.
    echo ℹ 沒有變更需要 commit（cache 內容跟上次相同）
)

echo.
echo === Step 3/3: git push origin main ===
echo.
git push origin main
if errorlevel 1 (
    echo.
    echo ✗ Push 失敗，請檢查網路或 GitHub 認證
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  ✓ 完成！Render 會自動偵測新 commit 並重新部署（約 3 分鐘）
echo  線上網址: https://stock-screener-pro-5wpz.onrender.com/
echo ============================================================
echo.
pause
