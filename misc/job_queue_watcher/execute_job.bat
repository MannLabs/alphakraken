@echo off
REM Windows batch script to execute jobs independently from the Python watcher
REM Usage: execute_job.bat "custom_command" "output_path" "raw_file_id"

setlocal enabledelayedexpansion

REM Read parameters from temporary file to avoid command line parsing issues
set "TEMP_FILE=%~1"

REM Read parameters from temp file (one per line)
set line_num=0
for /f "usebackq delims=" %%a in ("%TEMP_FILE%") do (
    set /a line_num+=1
    if !line_num!==1 set "CUSTOM_COMMAND=%%a"
    if !line_num!==2 set "OUTPUT_PATH=%%a"
    if !line_num!==3 set "RAW_FILE_ID=%%a"
)

REM Debug: Show parameters as received from temp file
echo DEBUG: Parameters read from temp file:
echo DEBUG: CUSTOM_COMMAND: [!CUSTOM_COMMAND!]
echo DEBUG: OUTPUT_PATH: [!OUTPUT_PATH!]
echo DEBUG: RAW_FILE_ID: [!RAW_FILE_ID!]
echo DEBUG: Temp file: %TEMP_FILE%
echo.

REM Create output directory if it doesn't exist - use delayed expansion for special chars
if not exist "!OUTPUT_PATH!" mkdir "!OUTPUT_PATH!"

REM Create log file path - use delayed expansion to handle special characters like $ and \
set "LOG_FILE=!OUTPUT_PATH!\job_status.log"

REM Get current timestamp
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set "timestamp=%datetime:~8,2%:%datetime:~10,2%:%datetime:~12,2%"

REM Write execution start to log - use delayed expansion to avoid issues with quotes
echo [%timestamp%] Executing: !CUSTOM_COMMAND! >> "%LOG_FILE%"

REM Execute the custom command using cmd /c to properly handle complex commands
REM This approach avoids issues with quotes and redirection operators in the command
cmd /c "!CUSTOM_COMMAND!" >> "%LOG_FILE%" 2>&1

REM Capture the exit code
set "EXIT_CODE=%ERRORLEVEL%"

REM Get timestamp again for completion message
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set "timestamp=%datetime:~8,2%:%datetime:~10,2%:%datetime:~12,2%"

REM Write completion status based on exit code
if %EXIT_CODE% == 0 (
    echo [%timestamp%] Process completed successfully ^(exit code: %EXIT_CODE%^) >> "%LOG_FILE%"
    echo COMPLETED >> "%LOG_FILE%"
) else (
    echo [%timestamp%] Process failed with exit code: %EXIT_CODE% >> "%LOG_FILE%"
    echo FAILED >> "%LOG_FILE%"
)

REM Clean up temporary file
if exist "%TEMP_FILE%" del "%TEMP_FILE%" >nul 2>&1

REM Exit with the same code as the original command
exit /b %EXIT_CODE%
