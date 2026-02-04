"""
Build script for Health Monitor Dashboard Application.

This script packages the web dashboard into a standalone executable using PyInstaller.
The resulting executable can be run without requiring Python to be installed.

Usage:
    python build_dashboard.py
    python build_dashboard.py --onedir    # Create a directory with all files
    python build_dashboard.py --clean     # Clean build artifacts before building
"""

import os
import sys
import shutil
import subprocess
import argparse

# Project paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(SCRIPT_DIR, 'dist')
BUILD_DIR = os.path.join(SCRIPT_DIR, 'build')

# Application info
APP_NAME = "HealthMonitorDashboard"
MAIN_SCRIPT = "run_dashboard.py"
ICON_FILE = None  # Set to .ico file path if you have an icon


def clean_build():
    """Remove previous build artifacts."""
    print("Cleaning previous build artifacts...")
    
    dirs_to_remove = [
        os.path.join(BUILD_DIR, APP_NAME),
        os.path.join(DIST_DIR, APP_NAME),
    ]
    
    files_to_remove = [
        f"{APP_NAME}.spec",
        os.path.join(DIST_DIR, f"{APP_NAME}.exe"),
        os.path.join(DIST_DIR, "run_dashboard.bat"),
    ]
    
    for d in dirs_to_remove:
        if os.path.exists(d) and os.path.isdir(d):
            shutil.rmtree(d)
            print(f"  Removed dir: {d}")
    
    for f in files_to_remove:
        path = f if os.path.isabs(f) else os.path.join(SCRIPT_DIR, f)
        if os.path.exists(path) and os.path.isfile(path):
            os.remove(path)
            print(f"  Removed file: {path}")
    
    print("Clean complete.\n")


def build_app(onefile=True):
    """Build the application using PyInstaller."""
    
    print("=" * 60)
    print(f"  Building {APP_NAME}")
    print("=" * 60)
    
    # Check if PyInstaller is installed
    try:
        import PyInstaller
        print(f"PyInstaller version: {PyInstaller.__version__}")
    except ImportError:
        print("Error: PyInstaller is not installed.")
        print("Install it with: pip install pyinstaller")
        sys.exit(1)
    
    # Prepare data files to include
    # Format: 'source;destination' for Windows
    sep = ';' if sys.platform == 'win32' else ':'
    
    data_files = [
        f"src/web/templates{sep}src/web/templates",
        f"config{sep}config",
    ]
    
    # Check if static folder exists
    static_dir = os.path.join(SCRIPT_DIR, 'src', 'web', 'static')
    if os.path.exists(static_dir):
        data_files.append(f"src/web/static{sep}src/web/static")
    
    # Build PyInstaller command
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--name', APP_NAME,
        '--noconfirm',
        '--clean',
        # Add src/web to Python path so dashboard module can be found
        '--paths', 'src/web',
    ]
    
    # One file or one directory
    if onefile:
        cmd.append('--onefile')
    else:
        cmd.append('--onedir')
    
    # Add data files
    for data in data_files:
        cmd.extend(['--add-data', data])
    
    # Hidden imports that PyInstaller might miss
    hidden_imports = [
        'flask',
        'jinja2',
        'werkzeug',
        'click',
        'itsdangerous',
        'markupsafe',
        'dashboard',
        'json_storage',
    ]
    
    for imp in hidden_imports:
        cmd.extend(['--hidden-import', imp])
    
    # Add icon if available
    if ICON_FILE and os.path.exists(ICON_FILE):
        cmd.extend(['--icon', ICON_FILE])
    
    # Console mode (show console window)
    cmd.append('--console')
    
    # Main script
    cmd.append(MAIN_SCRIPT)
    
    print("\nRunning PyInstaller...")
    print(f"Command: {' '.join(cmd)}\n")
    
    # Run PyInstaller
    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    
    if result.returncode != 0:
        print("\nBuild failed!")
        sys.exit(1)
    
    # Print success message
    print("\n" + "=" * 60)
    print("  Build Successful!")
    print("=" * 60)
    
    if onefile:
        exe_path = os.path.join(DIST_DIR, f"{APP_NAME}.exe")
    else:
        exe_path = os.path.join(DIST_DIR, APP_NAME, f"{APP_NAME}.exe")
    
    print(f"\nExecutable location: {exe_path}")
    print(f"\nTo run the dashboard:")
    print(f"  {APP_NAME}.exe -m <metrics_directory>")
    print(f"\nExample:")
    print(f"  {APP_NAME}.exe -m D:\\ServiceHealthMatrixLogs")
    print(f"  {APP_NAME}.exe -m D:\\metrics --port 8080")


def create_run_script():
    """Create a batch script to easily run the application."""
    
    batch_content = '''@echo off
REM Health Monitor Dashboard Launcher
REM This script starts the web dashboard

setlocal

REM Default metrics directory - modify as needed
set METRICS_DIR=D:\\metrics

REM Default port
set PORT=5000

REM Check if metrics directory is provided as argument
if not "%~1"=="" set METRICS_DIR=%~1

echo ============================================================
echo   Health Monitor Dashboard
echo ============================================================
echo.
echo Metrics Directory: %METRICS_DIR%
echo Dashboard URL: http://127.0.0.1:%PORT%
echo.
echo Starting server...
echo.

HealthMonitorDashboard.exe -m "%METRICS_DIR%" -p %PORT%

pause
'''
    
    batch_path = os.path.join(DIST_DIR, 'run_dashboard.bat')
    
    # Create dist directory if it doesn't exist
    os.makedirs(DIST_DIR, exist_ok=True)
    
    with open(batch_path, 'w', encoding='utf-8') as f:
        f.write(batch_content)
    
    print(f"\nCreated launcher script: {batch_path}")


def main():
    parser = argparse.ArgumentParser(description='Build Health Monitor Dashboard')
    parser.add_argument('--onedir', action='store_true',
                        help='Create a directory with all files instead of single exe')
    parser.add_argument('--clean', action='store_true',
                        help='Clean build artifacts before building')
    parser.add_argument('--clean-only', action='store_true',
                        help='Only clean, do not build')
    
    args = parser.parse_args()
    
    os.chdir(SCRIPT_DIR)
    
    if args.clean or args.clean_only:
        clean_build()
    
    if args.clean_only:
        return
    
    build_app(onefile=not args.onedir)
    create_run_script()


if __name__ == '__main__':
    main()
