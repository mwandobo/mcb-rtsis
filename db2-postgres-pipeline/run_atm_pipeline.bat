@echo off
REM ATM Pipeline Runner - BOT Project
REM Windows batch script to run the ATM data pipeline

echo ========================================
echo ATM Data Pipeline - BOT Project
echo ========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and try again
    pause
    exit /b 1
)

REM Set environment variables if needed
REM set DB2_HOST=172.10.2.42
REM set PG_HOST=localhost
REM set RABBITMQ_HOST=localhost

echo Choose ATM pipeline option:
echo 1. Run Professional ATM Pipeline (with RabbitMQ and tracking)
echo 2. Run Simple ATM Pipeline (direct processing, for testing)
echo 3. Run ATM Queue Consumer (process existing queue messages)
echo 4. Exit
echo.

set /p choice="Enter your choice (1-4): "

if "%choice%"=="1" goto professional
if "%choice%"=="2" goto simple
if "%choice%"=="3" goto consumer
if "%choice%"=="4" goto exit
goto invalid

:professional
echo.
echo Starting Professional ATM Pipeline...
echo This will use RabbitMQ and tracking for production processing
echo.
python atm_pipeline_rabbitmq.py
goto end

:simple
echo.
echo Starting Simple ATM Pipeline...
echo This will process ATM data directly without RabbitMQ (good for testing)
echo.
python simple_atm_pipeline.py
goto end

:consumer
echo.
echo Starting ATM Queue Consumer...
echo This will process existing messages in the ATM queue
echo Press Ctrl+C to stop the consumer
echo.
python consume_atm_queue.py
goto end

:invalid
echo.
echo Invalid choice. Please enter 1, 2, 3, or 4.
echo.
goto start

:end
echo.
echo ATM Pipeline execution completed.
echo Check the logs above for any errors or issues.
echo.

:exit
pause