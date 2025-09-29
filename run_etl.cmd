@echo off
rem This script runs the Callio ETL process and logs its output.

rem Set console to UTF-8 to prevent character encoding errors
chcp 65001 > NUL
set PYTHONIOENCODING=utf-8

rem Change directory to the script's location to ensure paths are correct
cd /D "%~dp0"

rem Run the ETL command and append its output (both stdout and stderr) to a log file
echo Starting ETL run at %date% %time% >> callio_etl.log
python -m callio_etl --mode once --job all >> callio_etl.log 2>&1
echo Finished ETL run with exit code %errorlevel% at %date% %time% >> callio_etl.log
echo. >> callio_etl.log
