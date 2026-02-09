@echo off
REM Kill existing gateway process on port 9301, then start a new one.
echo Stopping gateway...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr "LISTENING" ^| findstr ":9301"') do (
    taskkill /F /PID %%a >nul 2>&1
)
ping -n 2 127.0.0.1 >nul
echo Starting gateway...
start "ES Gateway" cmd /c "python -m gateway.main"
echo Gateway restarting on :9301
