@echo off
setlocal
chcp 65001 >nul
title Binance Futures Multi-Timeframe Scanner
cd /d "%~dp0"

echo ===============================================================
echo        BINANCE FUTURES MULTI-TIMEFRAME SCANNER
echo ===============================================================
echo.

set "PY_CMD="

if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" (
    set "PY_CMD=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
    goto found
)

where python >nul 2>nul
if not errorlevel 1 (
    set "PY_CMD=python"
    goto found
)

where py >nul 2>nul
if not errorlevel 1 (
    set "PY_CMD=py"
    goto found
)

echo [LOI] Khong tim thay Python tren may tinh!
echo Vui long cai dat Python hoac them Python vao PATH.
echo.
pause
exit /b 1

:found
echo [DANG KHOI CHAY] Dang bat giao dien Web Dashboard...
"%PY_CMD%" scanner.py

if errorlevel 1 (
    echo.
    echo [THONG BAO] Chuong trinh ket thuc.
)

echo.
pause
