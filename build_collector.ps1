# Build script for compiling collector_cli.py to standalone executable

# Install pyinstaller if not already installed
pip install pyinstaller psutil pyyaml

# Build single executable file
# --onefile: Create a single executable file
# --name: Name of the output executable
# --clean: Clean temporary files before building

pyinstaller --onefile --name health_collector --clean collector_cli.py

# The executable will be created in the 'dist' folder:
#   Windows: dist\health_collector.exe
#   Linux/macOS: dist/health_collector

Write-Host ""
Write-Host "Build completed!"
Write-Host "Executable location: dist\health_collector.exe"
Write-Host ""
Write-Host "Usage examples:"
Write-Host ""
Write-Host "  # Collect using PowerShell provider (default, auto-discovers local node)"
Write-Host "  .\dist\health_collector.exe --all"
Write-Host "  .\dist\health_collector.exe --all --provider powershell --cluster-name my-cluster"
Write-Host ""
Write-Host "  # Collect using file-based cluster config"
Write-Host "  .\dist\health_collector.exe --all --provider file --clusters-config config\clusters.yaml"
Write-Host ""
Write-Host "  # Collect for a specific node"
Write-Host "  .\dist\health_collector.exe --cluster production --node server-01"
Write-Host ""
Write-Host "  # Output to stdout as JSON"
Write-Host "  .\dist\health_collector.exe --all --stdout"
Write-Host ""
Write-Host "Windows Task Scheduler example:"
Write-Host "  Program: C:\HealthMonitor\dist\health_collector.exe"
Write-Host "  Arguments: --all --output-dir C:\HealthMonitor\data\metrics"
Write-Host "  Trigger: Daily, repeat every 1 minute"
