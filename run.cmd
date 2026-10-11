@echo off
rem ----- MediaScraper launcher -----
cd /d "%~dp0"
set "MS_PORT=8800"
set "MS_HOST=127.0.0.1"

rem use the interpreter that has requests / opencv / numpy / zhconv installed
set "PY=C:\Users\Administrator\AppData\Local\Programs\Python\Python314\python.exe"
if not exist "%PY%" set "PY=python"

echo.
echo   MediaScraper  -  http://%MS_HOST%:%MS_PORT%/
echo   (close this window to stop the service)
echo.
"%PY%" "%~dp0app.py"
pause
