"""
Standalone Metric Collector CLI

This is a standalone script for metric collection that can be compiled
into an executable and scheduled via cron/task scheduler.

Usage:
    python collector_cli.py --cluster <cluster_name> --provider powershell
    python collector_cli.py --cluster <cluster_name> --provider file --config config/clusters.yaml

Features:
    - Supports multiple cluster info providers (powershell, file)
    - Collects ClickHouse status metrics for all hosts in the specified cluster
    - Incrementally appends metrics to log files organized by cluster/node/date

Compile to executable:
    pip install pyinstaller
    pyinstaller --onefile collector_cli.py
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import cluster providers and collectors from src
try:
    from src.cluster.provider import (
        ClusterProviderFactory,
        ClusterInfoProvider,
        PowerShellClusterProvider,
        FileClusterProvider,
        Cluster,
        Node
    )
    from src.metrics.collector import (
        JsonMetricStorage,
        MetricValue,
        MetricCollector,
        get_all_collectors,
        ClickHouseStatusCollector,
    )
    IMPORTS_AVAILABLE = True
except ImportError as e:
    IMPORTS_AVAILABLE = False
    print(f"Error: Could not import required modules: {e}", file=sys.stderr)
    print("Make sure you're running from the project root directory.", file=sys.stderr)
    sys.exit(1)


# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging(verbose: bool = False) -> logging.Logger:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


# =============================================================================
# Collector Functions
# =============================================================================

def collect_metrics_for_node(node, cluster_name: str,
                              port: int,
                              logger: logging.Logger,
                              debug: bool = False) -> List[MetricValue]:
    """
    Collect all metrics for a single node.
    
    Args:
        node: Node object with name and host attributes
        cluster_name: Name of the cluster
        port: ClickHouse HTTP port
        logger: Logger instance
        debug: Enable debug mode to print curl commands and responses
    
    Returns:
        List of collected MetricValue objects
    """
    metrics = []
    # Create collectors for this specific node's host with debug mode
    collectors = get_all_collectors(host=node.host, port=port, debug=debug)
    
    for collector in collectors:
        try:
            metric = collector.collect(
                node_name=node.name,
                cluster_name=cluster_name
            )
            metrics.append(metric)
            logger.debug(f"  Collected {collector.name}: {metric.value}{metric.unit}")
        except Exception as e:
            logger.error(f"  Error collecting {collector.name} for {node.name}: {e}")
    return metrics


def create_provider(provider_type: str, cluster_name: str, config: Dict[str, Any],
                    logger: logging.Logger) -> Optional[ClusterInfoProvider]:
    """Create a cluster info provider based on type."""
    try:
        if provider_type == 'powershell':
            logger.info(f"Creating PowerShell provider for cluster: {cluster_name}")
            return PowerShellClusterProvider(
                cluster_name=cluster_name,
                dmclient_path=config.get('dmclient_path', '.\\dmclient.exe'),
                machine_function=config.get('machine_function', 'CH')
            )
        elif provider_type == 'file':
            file_path = config.get('config_path', 'config/clusters.yaml')
            logger.info(f"Creating File provider from: {file_path}")
            return FileClusterProvider(file_path=file_path)
        else:
            # Use factory for other types
            return ClusterProviderFactory.create(provider_type, config)
    except Exception as e:
        logger.error(f"Failed to create provider '{provider_type}': {e}")
        return None


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Metric Collector CLI - Collect ClickHouse status metrics for cluster hosts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect metrics using PowerShell provider (dmclient.exe)
  %(prog)s --cluster MTTitanMetricsBE-Prod-MWHE01 --provider powershell

  # Collect metrics using file-based cluster config
  %(prog)s --cluster my-cluster --provider file --config config/clusters.yaml

  # Specify custom dmclient directory path and machine function
  %(prog)s --cluster MTTitanMetricsBE-Prod-MWHE01 --provider powershell \\
           --dmclient-path "D:\\app\\APTools.ap_2026_01_25_25001\\" --machine-function CH

  # Output to stdout instead of log files
  %(prog)s --cluster my-cluster --provider file --stdout

  # Use custom output directory (default is D:\\metrics)
  %(prog)s --cluster my-cluster --provider powershell --output-dir E:\\logs\\metrics

  # Specify custom ClickHouse port
  %(prog)s --cluster my-cluster --provider file --port 8123

Provider Types:
  powershell  - Use dmclient.exe to get cluster info (parses CSV output)
  file        - Read cluster info from YAML/JSON file
        """
    )

    parser.add_argument('--cluster', '-c', required=True,
                        help='Cluster name to collect metrics for')
    parser.add_argument('--provider', '-p',
                        choices=['powershell', 'file'],
                        default='powershell',
                        help='Cluster information provider type (default: powershell)')
    parser.add_argument('--config',
                        default='config/clusters.yaml',
                        help='Path to clusters configuration file (for file provider)')
    parser.add_argument('--dmclient-path',
                        default='D:\\app\\APTools.ap_2026_01_25_25001\\',
                        help='Directory path containing dmclient.exe and environment files (for powershell provider)')
    parser.add_argument('--machine-function',
                        default='CH',
                        help='Machine function filter (for powershell provider, default: CH)')
    parser.add_argument('--port',
                        type=int,
                        default=8123,
                        help='ClickHouse HTTP port (default: 8123)')
    parser.add_argument('--output-dir', '-o',
                        default=r'D:\ServiceHealthMatrixLogs',
                        help='Output directory for JSON log files (default: D:\\ServiceHealthMatrixLogs)')
    parser.add_argument('--stdout', action='store_true',
                        help='Output metrics to stdout as JSON instead of log files')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Enable debug mode (print curl commands and responses)')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)
    logger.info(f"Starting metric collection for cluster: {args.cluster}")
    logger.info(f"Using provider: {args.provider}")

    # Build provider config
    provider_config = {
        'config_path': args.config,
        'dmclient_path': args.dmclient_path,
        'machine_function': args.machine_function,
    }

    # Create cluster provider
    provider = create_provider(args.provider, args.cluster, provider_config, logger)
    if not provider:
        logger.error("Failed to create cluster provider")
        sys.exit(1)

    # Get clusters
    clusters = provider.get_clusters()
    if not clusters:
        logger.error(f"No clusters found for '{args.cluster}' using {args.provider} provider")
        sys.exit(1)

    # Find the target cluster
    target_cluster = None
    for cluster in clusters:
        if cluster.name == args.cluster:
            target_cluster = cluster
            break
    
    if not target_cluster:
        # If using powershell provider, cluster is auto-created
        if clusters:
            target_cluster = clusters[0]
        else:
            logger.error(f"Cluster '{args.cluster}' not found")
            sys.exit(1)

    logger.info(f"Found cluster: {target_cluster.name} with {len(target_cluster.nodes)} nodes")
    logger.info(f"Collecting clickhouse_status metric on port {args.port}")
    if args.debug:
        logger.info("Debug mode enabled - curl commands and responses will be printed")

    # Initialize storage (using JsonMetricStorage from src.metrics.collector)
    storage = JsonMetricStorage(base_dir=args.output_dir)

    # Collect metrics for each node
    all_metrics: List[MetricValue] = []
    
    for node in target_cluster.nodes:
        logger.info(f"Collecting metrics for node: {node.name} (host: {node.host})")
        
        metrics = collect_metrics_for_node(node, target_cluster.name, args.port, logger, debug=args.debug)
        all_metrics.extend(metrics)

    # Output results
    if args.stdout:
        output = [m.to_dict() for m in all_metrics]
        print(json.dumps(output, indent=2, ensure_ascii=False))
    else:
        # Store all metrics to a single JSON file per cluster
        json_file = storage.store_batch(all_metrics)
        logger.info(f"Collection complete. Total metrics collected: {len(all_metrics)}")
        logger.info(f"Metrics saved to: {json_file}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
