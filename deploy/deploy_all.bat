:: Run all deploy scripts

@echo off

echo Running deploy_extract.bat...

call deploy_extract.bat

echo deploy_extract.bat complete.

echo Running deploy_load_staging.bat...

call deploy_load_staging.bat

echo deploy_load_staging.bat complete.

endlocal

pause