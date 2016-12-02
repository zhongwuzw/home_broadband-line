@echo off

REM Licensed to the CMRI

@setlocal

REM ==== START VALIDATION ====
if not "%JAVA_HOME%" == "" goto OkJHome

echo.
echo ERROR: JAVA_HOME not found in your environment.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation
echo.
goto error

:OkJHome
if exist "%JAVA_HOME%\bin\java.exe" if exist "%JAVA_HOME%\bin\javac.exe" goto setProjectHome

echo.
echo ERROR: JAVA_HOME is set to an invalid directory.
echo JAVA_HOME = %JAVA_HOME%
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation
echo.
goto error

:setProjectHome

rem ----- use the location of this script to infer $CMCC_MINA_HOME -------
if NOT "%OS%"=="Windows_NT" set DEFAULT_CMCC_MINA_HOME=..
if "%OS%"=="Windows_NT" set DEFAULT_CMCC_MINA_HOME=%~dp0\.
if "%OS%"=="WINNT" set DEFAULT_CMCC_MINA_HOME=%~dp0\.
if "%CMCC_MINA_HOME%"=="" set CMCC_MINA_HOME=%DEFAULT_CMCC_MINA_HOME%

echo "%CMCC_MINA_HOME%\lib"

rem ----- Create CLASSPATH --------------------------------------------
set CMCC_MINA_CLASSPATH=%CLASSPATH%;%CMCC_MINA_HOME%\bin
cd /d "%CMCC_MINA_HOME%\lib"
@setlocal enabledelayedexpansion
for /r %%i in (*.jar) do set CMCC_MINA_CLASSPATH=!CMCC_MINA_CLASSPATH!;%%i
@setlocal disabledelayedexpansion

cd /d %CMCC_MINA_HOME%

rem ----- Create SOURCEPATH --------------------------------------------
set CMCC_MINA_SOURCEPATH=%CMCC_MINA_HOME%\src
cd /d "%CMCC_MINA_HOME%\src"
del temp
del sourcepath
@setlocal enabledelayedexpansion
for /r %%j in (.) do echo %%j>>temp
findstr /e "main\\java\\\." temp >>sourcepath
for /f %%m in (sourcepath) do set CMCC_MINA_SOURCEPATH=!CMCC_MINA_SOURCEPATH!;%%m
@setlocal disabledelayedexpansion
del temp
del sourcepath

rem ----- Create SOURCEFILE --------------------------------------------
set CMCC_MINA_SOURCEFILE=%CMCC_MINA_HOME%\src
cd /d "%CMCC_MINA_SOURCEFILE%"
del temp
del sourcefile
@setlocal enabledelayedexpansion
for /r %%n in (*.java) do echo %%n>>temp
findstr /v "test\\java\\" temp >>sourcefile
@setlocal disabledelayedexpansion

rem ----- call java.. ---------------------------------------------------
@setlocal enabledelayedexpansion
set JAVA_CMD=!JAVA_HOME!\bin\javac
set BINPATH=%CMCC_MINA_HOME%\bin
set DISTPATH=%CMCC_MINA_HOME%\dist

echo "!CMCC_MINA_SOURCEPATH!"

echo "!CMCC_MINA_CLASSPATH!"

echo "!BINPATH!"

echo "!CMCC_MINA_SOURCEFILE!"

for /f %%l in (sourcefile) do (
set CMCC_MINA_SOURCEFILE=%%l
"!JAVA_CMD!" -sourcepath "!CMCC_MINA_SOURCEPATH!" -classpath "!CMCC_MINA_CLASSPATH!" -d "!BINPATH!" "!CMCC_MINA_SOURCEFILE!"
)
del temp
del sourcefile

cd /d "%DISTPATH%"
jar cvf cmcc-mina.jar -C %BINPATH% .
@setlocal disabledelayedexpansion


@endlocal
goto :eof

:error
echo.
echo Error information
echo.
goto :eof