@echo off
REM Build script for ParVuEx
REM Builds Launcher first, then ParVu

echo ========================================
echo Building ParVuEx
echo ========================================
echo.

REM Clean previous builds (optional - comment out if you want to keep them)
echo Cleaning previous builds...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
echo.

REM Build Launcher first
echo ========================================
echo Building Launcher...
echo ========================================
pyinstaller Launcher.spec
if errorlevel 1 (
    echo.
    echo ERROR: Launcher build failed!
    pause
    exit /b 1
)
echo.

REM Build ParVu
echo ========================================
echo Building ParVuEx...
echo ========================================
pyinstaller ParVuEx.spec
if errorlevel 1 (
    echo.
    echo ERROR: ParVuEx build failed!
    pause
    exit /b 1
)
echo.

REM Copy ParVuEx.exe to Launcher directory (if needed)
REM The launcher expects ParVuEx.exe in the same directory
if exist "dist\ParVuEx.exe" (
    echo Copying ParVuEx.exe to Launcher directory...
    copy /Y "dist\ParVuEx.exe" "dist\ParVuExLauncher\" >nul 2>&1
    if errorlevel 1 (
        echo Warning: Could not copy ParVuEx.exe to Launcher directory
    ) else (
        echo ParVuEx.exe copied successfully
    )
    echo.
)

echo ========================================
echo Build completed successfully!
echo ========================================
echo.
echo Output directories:
echo   - dist\ParVuExLauncher\  (Launcher executable)
echo   - dist\ParVuEx\             (ParVuEx executable)
echo.
pause

