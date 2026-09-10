@echo off
setlocal

rem Dummy quanting software to smoke-test a windows runner with the `simple_ssh` or `pueue`
rem engine: reports what the job handler passed in, waits, and reports one metric.
rem
rem Usage:
rem   run_dummy.cmd [any arguments]
rem
rem Set it as `software` of a settings entry with software type `custom` and metrics type
rem `custom`; the arguments come from `config_params`.
rem
rem Known limitation of the simple_ssh engine: its launcher invokes the executable without
rem `call`, so cmd.exe chains into this file and never writes the exit code file, and the job
rem is reported FAILED however this script ends. Under the pueue engine, which starts the task
rem from PowerShell, the exit code arrives correctly.

set SLEEP_SECONDS=20
set METRIC_NAME=dummy_metric
set METRIC_VALUE=0.42
set METRICS_FILE_NAME=metrics.csv

echo host:    %COMPUTERNAME%
echo user:    %USERNAME%
echo workdir: %CD%
echo args:    %*

rem the whole environment, not just the AlphaKraken variables: the point is to see what a
rem non-interactive session on the runner actually gets (PATH, proxies, conda, ...)
echo --- environment ---
set
echo --- end of environment ---

rem fails unless the mounts of the runner match the runner's `view`
dir /a "%RAW_FILE_PATH%" || echo RAW_FILE_PATH not readable
dir /a "%SETTINGS_PATH%" || echo SETTINGS_PATH not readable
dir /a "%OUTPUT_PATH%" || echo OUTPUT_PATH not readable

rem long enough for the sensor to see the job in state RUNNING
powershell -NoProfile -Command "Start-Sleep -Seconds %SLEEP_SECONDS%"

rem redirect first, so the metric names and values get no trailing blank
> "%OUTPUT_PATH%\%METRICS_FILE_NAME%" echo %METRIC_NAME%
>> "%OUTPUT_PATH%\%METRICS_FILE_NAME%" echo %METRIC_VALUE%

echo done
exit /b 0
