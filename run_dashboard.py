"""
Run Health Monitor Web Dashboard.

This script starts the web server for visualizing cluster health metrics.
Metrics directory can be specified via command line arguments.

Usage:
    python run_dashboard.py                           # Use default D:\\metrics
    python run_dashboard.py -m D:\\ServiceHealthMatrixLogs
    python run_dashboard.py --metrics-dir /path/to/metrics --port 8080
"""

import os
import sys
import argparse

# Get absolute path of the script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Add src/web directory to path for imports
web_dir = os.path.join(SCRIPT_DIR, 'src', 'web')
sys.path.insert(0, web_dir)

from dashboard import run_server

# Default metrics directory
DEFAULT_METRICS_DIR = r"D:\metrics"


def main():
    parser = argparse.ArgumentParser(
        description='Health Monitor Web Dashboard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_dashboard.py
  python run_dashboard.py -m D:\\ServiceHealthMatrixLogs
  python run_dashboard.py --metrics-dir ./tests/sample_metrics --port 8080
  python run_dashboard.py -m D:\\metrics --host 0.0.0.0 --port 5000
        """
    )
    
    parser.add_argument('-m', '--metrics-dir',
                        default=DEFAULT_METRICS_DIR,
                        help=f'Directory containing JSON metric logs (default: {DEFAULT_METRICS_DIR})')
    parser.add_argument('--host',
                        default='127.0.0.1',
                        help='Host address to bind (default: 127.0.0.1)')
    parser.add_argument('-p', '--port',
                        type=int,
                        default=5000,
                        help='Port number (default: 5000)')
    parser.add_argument('--debug',
                        action='store_true',
                        help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Convert to absolute path if relative
    metrics_dir = os.path.abspath(args.metrics_dir)
    
    if not os.path.exists(metrics_dir):
        print(f"Warning: Metrics directory not found: {metrics_dir}")
        print("The directory will be created when metrics are collected.")
        print("You can also specify a different directory with -m option.\n")
    
    print("=" * 60)
    print("  Health Monitor Web Dashboard")
    print("=" * 60)
    print(f"\nMetrics directory: {metrics_dir}")
    print(f"\nOpen your browser and navigate to: http://{args.host}:{args.port}")
    print("\nPress Ctrl+C to stop the server.\n")
    
    run_server(metrics_dir, host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()
