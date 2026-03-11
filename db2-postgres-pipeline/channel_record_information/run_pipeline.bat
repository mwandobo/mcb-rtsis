@echo off
REM Channel Record Information Pipeline Runner for Windows
REM This script sets up the environment and runs the pipeline

echo ========================================
echo Channel Record Information Pipeline Runner
echo ========================================

REM Change to the pipeline directory
cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

echo Python version:
python --version

REM Check if required packages are installed
echo.
echo Checking required packages...
python -c "import pika, psycopg2, pyodbc" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Required packages not installed
    echo Please install: pip install pika psycopg2-binary pyodbc
    pause
    exit /b 1
)

echo Required packages are installed.

REM Create logs directory if it doesn't exist
if not exist "..\logs" mkdir "..\logs"

REM Menu for user selection
echo.
echo Select an option:
echo 1. Create PostgreSQL table
echo 2. Run test suite
echo 3. Run streaming pipeline
echo 4. Clear RabbitMQ queue
echo 5. Verify data quality
echo 6. Exit
echo.
set /p choice="Enter your choice (1-6): "

if "%choice%"=="1" goto create_table
if "%choice%"=="2" goto run_tests
if "%choice%"=="3" goto run_pipeline
if "%choice%"=="4" goto clear_queue
if "%choice%"=="5" goto verify_data
if "%choice%"=="6" goto exit
goto invalid_choice

:create_table
echo.
echo Creating PostgreSQL table...
python create_channel_record_information_table.py
if errorlevel 1 (
    echo ERROR: Table creation failed
    pause
    exit /b 1
)
echo Table created successfully!
goto menu_end

:run_tests
echo.
echo Running test suite...
python test_channel_record_information_pipeline.py
if errorlevel 1 (
    echo ERROR: Tests failed
    pause
    exit /b 1
)
echo Tests completed!
goto menu_end

:run_pipeline
echo.
echo Starting Channel Record Information Streaming Pipeline...
echo Press Ctrl+C to stop the pipeline
echo.
python run_channel_record_information_pipeline.py
goto menu_end

:clear_queue
echo.
echo Clearing RabbitMQ queue...
python clear_channel_record_information_queue.py
if errorlevel 1 (
    echo ERROR: Queue clearing failed
    pause
    exit /b 1
)
echo Queue cleared successfully!
goto menu_end

:verify_data
echo.
echo Verifying data quality...
python verify_data_quality.py
if errorlevel 1 (
    echo ERROR: Data verification failed
    pause
    exit /b 1
)
echo Data verification completed!
goto menu_end

:invalid_choice
echo Invalid choice. Please select 1-6.
goto menu_end

:menu_end
echo.
pause
goto :eof

:exit
echo Goodbye!