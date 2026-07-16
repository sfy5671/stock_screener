@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Stock Screener - Local
echo ============================================================
echo   Stock Screener  (Local personal analysis center)
echo   Starting local server, browser opens in a few seconds...
echo   Keep this window open while using. Close it to stop.
echo ============================================================
echo.
start "" cmd /c "timeout /t 4 >nul & start http://127.0.0.1:5000"
python app.py
