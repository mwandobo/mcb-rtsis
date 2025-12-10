@echo off
REM Deploy Kafka Connect connectors for DB2 to PostgreSQL sync
REM Usage: deploy-connectors.bat [source|sink|all]

setlocal enabledelayedexpansion

set CONNECT_URL=http://localhost:8083
set SCRIPT_DIR=%~dp0
set CONFIG_DIR=%SCRIPT_DIR%..\config\connectors

echo ============================================
echo  Kafka Connect Connector Deployment
echo ============================================
echo.

REM Check if Kafka Connect is available
echo Checking Kafka Connect availability...
curl -s %CONNECT_URL%/ >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Kafka Connect is not available at %CONNECT_URL%
    echo Make sure docker-compose is running: docker-compose up -d
    exit /b 1
)
echo [OK] Kafka Connect is available
echo.

set ACTION=%1
if "%ACTION%"=="" set ACTION=all

if "%ACTION%"=="source" goto deploy_source
if "%ACTION%"=="sink" goto deploy_sink
if "%ACTION%"=="all" goto deploy_all
echo Usage: deploy-connectors.bat [source^|sink^|all]
exit /b 1

:deploy_source
echo Deploying SOURCE connectors...
echo.
for %%f in (%CONFIG_DIR%\source\*.json) do (
    echo Deploying: %%~nxf
    curl -s -X POST -H "Content-Type: application/json" -d @"%%f" %CONNECT_URL%/connectors
    echo.
    timeout /t 2 /nobreak >nul
)
goto end

:deploy_sink
echo Deploying SINK connectors...
echo.
for %%f in (%CONFIG_DIR%\sink\*.json) do (
    echo Deploying: %%~nxf
    curl -s -X POST -H "Content-Type: application/json" -d @"%%f" %CONNECT_URL%/connectors
    echo.
)
goto end

:deploy_all
call :deploy_source
call :deploy_sink
goto end

:end
echo.
echo ============================================
echo  Deployment Complete!
echo ============================================
echo.
echo Check status: curl %CONNECT_URL%/connectors
echo Kafka UI: http://localhost:8080
