@echo off
REM Run Pipeline Scheduler
REM This script starts the scheduled pipeline runner

echo ============================================
echo Pipeline Scheduler
echo ============================================
echo.

cd /d "%~dp0"

REM Activate virtual environment if exists
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Run the scheduler
python schedule_pipelines.py

pause