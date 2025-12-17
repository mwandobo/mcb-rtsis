@echo off
echo PostgreSQL Table Creation Script
echo =================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python and try again
    pause
    exit /b 1
)

REM Check if required packages are installed
echo Checking required packages...
python -c "import psycopg2, dotenv" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install psycopg2-binary python-dotenv
    if errorlevel 1 (
        echo Error: Failed to install required packages
        pause
        exit /b 1
    )
)

echo.
echo Creating PostgreSQL tables...
echo.
python create_tables.py

if errorlevel 1 (
    echo.
    echo Table creation failed! Check the error messages above.
) else (
    echo.
    echo Table creation completed successfully!
)

echo.
echo Press any key to exit...
pause >nul