@echo off

REM Check if the correct number of arguments is provided
if "%1" == "" (
    echo Usage: %0 ^<port1^> [^<port2^> ^<port3^> ...^]
    exit /b 1
)

REM Loop through each port number provided as arguments
for %%a in (%*) do (
    REM Start python node.py with the specified port in a new terminal
    start cmd /c python node.py %%a
)

REM Sleep to allow time for nodes to start (adjust as needed)
timeout /t 5 >nul

REM Run server.py in a new terminal
start cmd /c python server.py