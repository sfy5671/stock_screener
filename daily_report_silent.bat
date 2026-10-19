@echo off
REM ============================================================
REM  Daily Stock Report - AI watchlist + flyers, push to Telegram
REM  Runs after daily_refresh (data must be fresh first).
REM ============================================================
chcp 65001 >nul
cd /d "%~dp0"

set LOG=.cache\daily_report.log

echo. >> "%LOG%"
echo ============================================================ >> "%LOG%"
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMddHHmmss"') do set DT=%%I
set NOW=%DT:~0,4%-%DT:~4,2%-%DT:~6,2% %DT:~8,2%:%DT:~10,2%:%DT:~12,2%
echo  Daily Report  %NOW% >> "%LOG%"
echo ============================================================ >> "%LOG%"

python daily_report\run_daily_report.py >> "%LOG%" 2>&1
if errorlevel 1 (
    echo ERROR: run_daily_report.py failed >> "%LOG%"
    exit /b 1
)

echo OK: Done >> "%LOG%"
exit /b 0
