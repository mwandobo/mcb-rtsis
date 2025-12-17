@echo off
echo PostgreSQL Connection Test
echo ========================

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
echo Running simple connection test...
echo.
python simple_pg_test.py

echo.
echo Running detailed connection test...
echo.
python test_postgres_connection.py

echo.
echo Test completed. Press any key to exit...
pause >nul