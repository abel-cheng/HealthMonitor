#!/bin/bash
# Build script for compiling collector_cli.py to standalone executable (Linux/macOS)

# Install pyinstaller if not already installed
pip install pyinstaller psutil pyyaml

# Build single executable file
pyinstaller --onefile --name health_collector --clean collector_cli.py

echo ""
echo "Build completed!"
echo "Executable location: dist/health_collector"
echo ""
echo "Usage examples:"
echo ""
echo "  # Collect using PowerShell provider (Windows default)"
echo "  ./dist/health_collector --all --provider powershell"
echo ""
echo "  # Collect using file-based cluster config"
echo "  ./dist/health_collector --all --provider file --clusters-config config/clusters.yaml"
echo ""
echo "  # Collect using environment variables"
echo "  export HEALTH_CLUSTER_NAME=production"
echo "  export HEALTH_NODE_NAME=server-01"
echo "  ./dist/health_collector --all --provider env"
echo ""
echo "  # Collect for a specific node"
echo "  ./dist/health_collector --cluster production --node server-01"
echo ""
echo "  # Output to stdout as JSON"
echo "  ./dist/health_collector --all --stdout"
echo ""
echo "Cron job examples:"
echo "  # Collect every minute using file provider"
echo "  * * * * * /opt/healthmonitor/health_collector --all -p file --clusters-config /etc/healthmonitor/clusters.yaml -o /var/lib/metrics"
echo ""
echo "  # Collect every 5 minutes using environment variables"
echo "  */5 * * * * HEALTH_CLUSTER_NAME=prod HEALTH_NODE_NAME=\$(hostname) /opt/healthmonitor/health_collector --all -p env -o /var/lib/metrics"
