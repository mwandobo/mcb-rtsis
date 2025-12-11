@echo off
REM Start the complete DB2 to PostgreSQL sync pipeline
REM Usage: start-all.bat

echo ============================================
echo  DB2 to PostgreSQL Sync Pipeline
echo ============================================
echo.

cd /d %~dp0..

echo Step 1: Checking Docker...
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not installed or not running
    echo Please install Docker Desktop and try again
    pause
    exit /b 1
)
echo [OK] Docker is available
echo.

echo Step 2: Checking JDBC drivers...
if not exist "jdbc-drivers\db2jcc4.jar" (
    echo [ERROR] DB2 JDBC driver not found!
    echo Please copy db2jcc4.jar to kafka-connect\jdbc-drivers\
    echo.
    echo Run this command:
    echo copy "C:\Program Files\IBM\SQLLIB\java\db2jcc4.jar" jdbc-drivers\
    pause
    exit /b 1
)
echo [OK] DB2 JDBC driver found
echo.

echo Step 3: Starting Docker containers...
docker-compose up -d
echo.

echo Step 4: Waiting for services to start (60 seconds)...
timeout /t 60 /nobreak

echo.
echo Step 5: Checking Kafka Connect...
:wait_connect
curl -s http://localhost:8083/ >nul 2>&1
if errorlevel 1 (
    echo Waiting for Kafka Connect...
    timeout /t 10 /nobreak >nul
    goto wait_connect
)
echo [OK] Kafka Connect is ready
echo.

echo Step 6: Deploying connectors...
call scripts\deploy-connectors.bat all
echo.

echo ============================================
echo  Pipeline Started Successfully!
echo ============================================
echo.
echo  Kafka UI:     http://localhost:8080
echo  Connect API:  http://localhost:8083
echo  PostgreSQL:   localhost:5432
echo.
echo  To check status: scripts\check-status.bat
echo  To stop: docker-compose down
echo ============================================

pause
