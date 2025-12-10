@echo off
REM Check status of all Kafka Connect connectors
REM Usage: check-status.bat

set CONNECT_URL=http://localhost:8083

echo ============================================
echo  Kafka Connect Status Check
echo ============================================
echo.

echo Checking Kafka Connect...
curl -s %CONNECT_URL%/ >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Kafka Connect is not available
    exit /b 1
)

echo.
echo Installed Connectors:
echo ---------------------
curl -s %CONNECT_URL%/connectors
echo.
echo.

echo Connector Status:
echo -----------------
for /f "tokens=*" %%c in ('curl -s %CONNECT_URL%/connectors ^| findstr /r "[a-z]"') do (
    echo.
    echo Checking connector status...
)

echo.
echo Detailed status for each connector:
echo.

curl -s %CONNECT_URL%/connectors/db2-cash-information-source/status 2>nul
echo.
curl -s %CONNECT_URL%/connectors/db2-loan-information-source/status 2>nul
echo.
curl -s %CONNECT_URL%/connectors/postgres-bank-sink/status 2>nul
echo.

echo ============================================
echo  Open Kafka UI for full dashboard:
echo  http://localhost:8080
echo ============================================
