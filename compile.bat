@echo off

cd /d %0\..

SET Lib_Name=ResultSetConverter
SET Lib_Source_Dir="%CD%"
SET B4J_SLC=C:\Apps\B4A\Tools\SimpleLibraryCompiler\B4J_LibraryCompiler.exe

RMDIR %Lib_Source_Dir%\bin /Q /S

echo Library Name = %Lib_Name%
echo Library Source Directory = %Lib_Source_Dir%
echo B4J Library Compiler = %B4J_SLC%
echo.

REM ------------------------------------------
REM Compile library
REM ------------------------------------------
echo.

set errorlevel=
%B4J_SLC% %Lib_Name% %Lib_Source_Dir%

IF "%errorlevel%"=="0" GOTO OK
IF "%errorlevel%"=="1" GOTO ERRORS

:OK
echo.
echo Done!
echo.
GOTO EXIT

:ERRORS
color FC
echo.
echo ******************************************
echo There were compile errors!
echo ******************************************
echo.
echo Window will close in 15 seconds...
timeout /t 15 /nobreak >nul

:EXIT