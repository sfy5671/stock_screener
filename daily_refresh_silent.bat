@echo off
REM ============================================================
REM  Stock Screener - Daily Refresh (silent mode for Task Scheduler)
REM ============================================================
chcp 65001 >nul
cd /d "%~dp0"

set LOG=.cache\daily_refresh.log

echo. >> "%LOG%"
echo ============================================================ >> "%LOG%"
for /f "tokens=2 delims==" %%I in ('"wmic os get localdatetime /value"') do set DT=%%I
set TODAY=%DT:~0,4%-%DT:~4,2%-%DT:~6,2%
set NOW=%DT:~0,4%-%DT:~4,2%-%DT:~6,2% %DT:~8,2%:%DT:~10,2%:%DT:~12,2%
echo  Stock Screener Auto Refresh  %NOW% >> "%LOG%"
echo ============================================================ >> "%LOG%"

echo [Step 1] daily_refresh.py >> "%LOG%"
python daily_refresh.py >> "%LOG%" 2>&1
if errorlevel 1 (
    echo ERROR: daily_refresh.py failed >> "%LOG%"
    exit /b 1
)

echo. >> "%LOG%"
echo [Step 2] git add and commit >> "%LOG%"
git add .cache/basic_indicators.json .cache/company_capital.json .cache/inst_hist_5.json .cache/margin.json >> "%LOG%" 2>&1
git commit -m "daily data refresh %TODAY%" >> "%LOG%" 2>&1

echo. >> "%LOG%"
echo [Step 3] git push origin main >> "%LOG%"
git push origin main >> "%LOG%" 2>&1
if errorlevel 1 (
    echo ERROR: git push failed >> "%LOG%"
    exit /b 1
)

echo. >> "%LOG%"
echo OK: Done >> "%LOG%"
exit /b 0
