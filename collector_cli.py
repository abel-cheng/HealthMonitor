"""
Standalone Metric Collector CLI

This is a standalone script for metric collection that can be compiled
into an executable and scheduled via cron/task scheduler.

Usage:
    python collector_cli.py --cluster <cluster_name> --node <node_name>
    python collector_cli.py --all --provider powershell
    python collector_cli.py --all --provider file --clusters-config config/clusters.yaml

Compile to executable:
    pip install pyinstaller
    pyinstaller --onefile collector_cli.py
"""

import os
import sys
import json
import argparse
import uuid
import subprocess
import socket
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod

# Try to import psutil, handle if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("Warning: psutil not available, using mock data")

# Try to import yaml, use json fallback if not available
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class Node:
    """Represents a node in a cluster."""
    name: str
    type: str
    host: str
    collection_method: str
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Cluster:
    """Represents a cluster with multiple nodes."""
    name: str
    description: str
    nodes: List[Node] = field(default_factory=list)


@dataclass
class MetricValue:
    """Represents a single metric measurement."""
    metric_id: str
    metric_name: str
    value: Any
    timestamp: str
    node_name: str
    cluster_name: str
    unit: str = ""
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MetricCollector:
    """Base class for metric collectors."""

    def __init__(self, name: str, unit: str = ""):
        self.name = name
        self.unit = unit

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        raise NotImplementedError

    def _create_metric(self, node_name: str, cluster_name: str, value: Any) -> MetricValue:
        return MetricValue(
            metric_id=str(uuid.uuid4()),
            metric_name=self.name,
            value=value,
            timestamp=datetime.utcnow().isoformat(),
            node_name=node_name,
            cluster_name=cluster_name,
            unit=self.unit
        )


class CPUPercentCollector(MetricCollector):
    def __init__(self):
        super().__init__(name="cpu_percent", unit="%")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.cpu_percent(interval=1)
        else:
            value = 0.0
        return self._create_metric(node_name, cluster_name, value)


class MemoryPercentCollector(MetricCollector):
    def __init__(self):
        super().__init__(name="memory_percent", unit="%")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.virtual_memory().percent
        else:
            value = 0.0
        return self._create_metric(node_name, cluster_name, value)


class MemoryUsedCollector(MetricCollector):
    def __init__(self):
        super().__init__(name="memory_used", unit="bytes")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.virtual_memory().used
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class DiskPercentCollector(MetricCollector):
    def __init__(self, path: str = None):
        super().__init__(name="disk_percent", unit="%")
        self.path = path or ("C:\\" if os.name == 'nt' else "/")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            try:
                value = psutil.disk_usage(self.path).percent
            except Exception:
                value = 0.0
        else:
            value = 0.0
        return self._create_metric(node_name, cluster_name, value)


class DiskUsedCollector(MetricCollector):
    def __init__(self, path: str = None):
        super().__init__(name="disk_used", unit="bytes")
        self.path = path or ("C:\\" if os.name == 'nt' else "/")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            try:
                value = psutil.disk_usage(self.path).used
            except Exception:
                value = 0
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class NetworkBytesRecvCollector(MetricCollector):
    def __init__(self):
        super().__init__(name="network_bytes_recv", unit="bytes")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.net_io_counters().bytes_recv
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class NetworkBytesSentCollector(MetricCollector):
    def __init__(self):
        super().__init__(name="network_bytes_sent", unit="bytes")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.net_io_counters().bytes_sent
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class NodeStatusCollector(MetricCollector):
    def __init__(self):
        super().__init__(name="node_status", unit="")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        # Node is up if this collector runs successfully
        return self._create_metric(node_name, cluster_name, 1)


class LoadAverageCollector(MetricCollector):
    def __init__(self):
        super().__init__(name="load_average", unit="")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            try:
                value = psutil.getloadavg()[0]
            except (AttributeError, OSError):
                # getloadavg not available on Windows
                value = psutil.cpu_percent() / 100.0 * psutil.cpu_count()
        else:
            value = 0.0
        return self._create_metric(node_name, cluster_name, value)


class ProcessCountCollector(MetricCollector):
    def __init__(self):
        super().__init__(name="process_count", unit="")

    def collect(self, node_name: str, cluster_name: str) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = len(psutil.pids())
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


# =============================================================================
# Cluster Information Providers
# =============================================================================

class ClusterInfoProvider(ABC):
    """Abstract base class for cluster information providers."""

    @abstractmethod
    def get_clusters(self) -> List[Cluster]:
        """Retrieve all clusters."""
        pass

    @abstractmethod
    def refresh(self) -> None:
        """Refresh cluster information from the source."""
        pass


class FileClusterProvider(ClusterInfoProvider):
    """Cluster information provider that reads from a YAML/JSON file."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self._clusters: List[Cluster] = []
        self.refresh()

    def get_clusters(self) -> List[Cluster]:
        return self._clusters

    def refresh(self) -> None:
        self._clusters = []
        if not os.path.exists(self.file_path):
            print(f"Warning: Cluster config file not found: {self.file_path}",
                  file=sys.stderr)
            return

        with open(self.file_path, 'r', encoding='utf-8') as f:
            if self.file_path.endswith(('.yaml', '.yml')):
                if YAML_AVAILABLE:
                    data = yaml.safe_load(f)
                else:
                    print("Warning: YAML support not available", file=sys.stderr)
                    return
            else:
                data = json.load(f)

        if not data or 'clusters' not in data:
            return

        for cluster_data in data['clusters']:
            nodes = []
            for node_data in cluster_data.get('nodes', []):
                node = Node(
                    name=node_data['name'],
                    type=node_data.get('type', 'worker'),
                    host=node_data.get('host', 'localhost'),
                    collection_method=node_data.get('collection_method', 'local'),
                    attributes=node_data.get('attributes', {})
                )
                nodes.append(node)

            cluster = Cluster(
                name=cluster_data['name'],
                description=cluster_data.get('description', ''),
                nodes=nodes
            )
            self._clusters.append(cluster)


class PowerShellClusterProvider(ClusterInfoProvider):
    """Cluster information provider that uses PowerShell commands."""

    def __init__(self, script_path: Optional[str] = None, cluster_name: Optional[str] = None):
        self.script_path = script_path
        self.cluster_name = cluster_name or "local-cluster"
        self._clusters: List[Cluster] = []
        self.refresh()

    def get_clusters(self) -> List[Cluster]:
        return self._clusters

    def refresh(self) -> None:
        self._clusters = []
        try:
            if self.script_path and os.path.exists(self.script_path):
                # Execute custom PowerShell script
                result = subprocess.run(
                    ['powershell', '-ExecutionPolicy', 'Bypass', '-File', self.script_path],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                if result.returncode == 0 and result.stdout:
                    data = json.loads(result.stdout)
                    self._parse_cluster_data(data)
                    return
            
            # Default: Get local computer info via PowerShell
            self._get_local_cluster_info()

        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            print(f"Warning: PowerShell provider error: {e}", file=sys.stderr)
            # Fallback to basic local info
            self._get_fallback_cluster_info()

    def _get_local_cluster_info(self) -> None:
        """Get cluster info from local computer using PowerShell."""
        ps_command = '''
        $computerName = $env:COMPUTERNAME
        $domain = $env:USERDOMAIN
        $ip = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -ne '127.0.0.1' } | Select-Object -First 1).IPAddress
        if (-not $ip) { $ip = "localhost" }
        
        $result = @{
            clusters = @(
                @{
                    name = $env:CLUSTER_NAME
                    description = "Cluster discovered via PowerShell"
                    nodes = @(
                        @{
                            name = $computerName
                            type = "worker"
                            host = $ip
                            collection_method = "local"
                            attributes = @{
                                domain = $domain
                                os = [System.Environment]::OSVersion.VersionString
                            }
                        }
                    )
                }
            )
        }
        $result | ConvertTo-Json -Depth 10 -Compress
        '''
        
        # Set cluster name environment variable for the PowerShell script
        env = os.environ.copy()
        env['CLUSTER_NAME'] = self.cluster_name
        
        try:
            result = subprocess.run(
                ['powershell', '-Command', ps_command],
                capture_output=True,
                text=True,
                timeout=30,
                env=env
            )

            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout.strip())
                self._parse_cluster_data(data)
            else:
                self._get_fallback_cluster_info()
        except Exception:
            self._get_fallback_cluster_info()

    def _get_fallback_cluster_info(self) -> None:
        """Fallback cluster info when PowerShell fails."""
        hostname = socket.gethostname()
        try:
            host_ip = socket.gethostbyname(hostname)
        except socket.gaierror:
            host_ip = "localhost"

        node = Node(
            name=hostname,
            type="worker",
            host=host_ip,
            collection_method="local"
        )
        cluster = Cluster(
            name=self.cluster_name,
            description="Local cluster (fallback)",
            nodes=[node]
        )
        self._clusters.append(cluster)

    def _parse_cluster_data(self, data: Dict) -> None:
        """Parse cluster data from PowerShell output."""
        if 'clusters' not in data:
            return

        for cluster_data in data['clusters']:
            nodes = []
            for node_data in cluster_data.get('nodes', []):
                node = Node(
                    name=node_data.get('name', 'unknown'),
                    type=node_data.get('type', 'worker'),
                    host=node_data.get('host', 'localhost'),
                    collection_method=node_data.get('collection_method', 'local'),
                    attributes=node_data.get('attributes', {})
                )
                nodes.append(node)

            cluster = Cluster(
                name=cluster_data.get('name', 'unknown'),
                description=cluster_data.get('description', ''),
                nodes=nodes
            )
            self._clusters.append(cluster)


class EnvironmentClusterProvider(ClusterInfoProvider):
    """Cluster information provider that reads from environment variables."""

    def __init__(self):
        self._clusters: List[Cluster] = []
        self.refresh()

    def get_clusters(self) -> List[Cluster]:
        return self._clusters

    def refresh(self) -> None:
        self._clusters = []
        
        cluster_name = os.environ.get('HEALTH_CLUSTER_NAME', 'default-cluster')
        node_name = os.environ.get('HEALTH_NODE_NAME', socket.gethostname())
        node_type = os.environ.get('HEALTH_NODE_TYPE', 'worker')
        node_host = os.environ.get('HEALTH_NODE_HOST', 'localhost')

        node = Node(
            name=node_name,
            type=node_type,
            host=node_host,
            collection_method="local"
        )
        cluster = Cluster(
            name=cluster_name,
            description="Cluster from environment variables",
            nodes=[node]
        )
        self._clusters.append(cluster)


def create_cluster_provider(provider_type: str, **kwargs) -> ClusterInfoProvider:
    """Factory function to create cluster providers."""
    if provider_type == 'file':
        return FileClusterProvider(
            file_path=kwargs.get('config_path', 'config/clusters.yaml')
        )
    elif provider_type == 'powershell':
        return PowerShellClusterProvider(
            script_path=kwargs.get('script_path'),
            cluster_name=kwargs.get('cluster_name', 'local-cluster')
        )
    elif provider_type == 'env':
        return EnvironmentClusterProvider()
    else:
        raise ValueError(f"Unknown provider type: {provider_type}")


# =============================================================================
# Metric Storage
# =============================================================================

class MetricStorage:
    """Handles storage of metrics to JSON files organized by hour."""

    def __init__(self, base_dir: str = "data/metrics"):
        self.base_dir = base_dir

    def _get_file_path(self, cluster_name: str, node_name: str, timestamp: datetime) -> str:
        """Generate file path based on cluster, node, and hour."""
        date_dir = timestamp.strftime("%Y/%m/%d")
        hour_file = timestamp.strftime("%H") + ".json"
        dir_path = os.path.join(self.base_dir, cluster_name, node_name, date_dir)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, hour_file)

    def store(self, metric: MetricValue) -> None:
        """Store a single metric value."""
        timestamp = datetime.fromisoformat(metric.timestamp)
        file_path = self._get_file_path(metric.cluster_name, metric.node_name, timestamp)

        metrics = []
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    metrics = json.load(f)
            except (json.JSONDecodeError, IOError):
                metrics = []

        metrics.append(metric.to_dict())

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2)

    def store_batch(self, metrics: List[MetricValue]) -> None:
        """Store multiple metrics efficiently."""
        for metric in metrics:
            self.store(metric)


def get_all_collectors() -> List[MetricCollector]:
    """Get all available metric collectors."""
    return [
        CPUPercentCollector(),
        MemoryPercentCollector(),
        MemoryUsedCollector(),
        DiskPercentCollector(),
        DiskUsedCollector(),
        NetworkBytesRecvCollector(),
        NetworkBytesSentCollector(),
        NodeStatusCollector(),
        LoadAverageCollector(),
        ProcessCountCollector(),
    ]


def collect_metrics(node_name: str, cluster_name: str,
                    collectors: List[MetricCollector] = None) -> List[MetricValue]:
    """Collect all metrics for a node."""
    if collectors is None:
        collectors = get_all_collectors()

    metrics = []
    for collector in collectors:
        try:
            metric = collector.collect(node_name, cluster_name)
            metrics.append(metric)
        except Exception as e:
            print(f"Error collecting {collector.name}: {e}", file=sys.stderr)
    return metrics


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML or JSON file."""
    if not os.path.exists(config_path):
        return {}

    with open(config_path, 'r', encoding='utf-8') as f:
        if config_path.endswith('.yaml') or config_path.endswith('.yml'):
            if YAML_AVAILABLE:
                return yaml.safe_load(f) or {}
            else:
                print("Warning: YAML support not available, please use JSON config",
                      file=sys.stderr)
                return {}
        else:
            return json.load(f)


def main():
    parser = argparse.ArgumentParser(
        description='Standalone Metric Collector CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect metrics for a specific node
  %(prog)s --cluster production --node server-01

  # Collect metrics for all nodes using PowerShell provider (default)
  %(prog)s --all

  # Collect metrics using PowerShell with custom cluster name
  %(prog)s --all --provider powershell --cluster-name my-cluster

  # Collect metrics using file-based cluster config
  %(prog)s --all --provider file --clusters-config config/clusters.yaml

  # Collect metrics using environment variables
  %(prog)s --all --provider env

  # Output to stdout instead of storing to file
  %(prog)s --cluster production --node server-01 --stdout

  # Use custom storage directory
  %(prog)s --cluster production --node server-01 --output-dir /var/lib/metrics

Cluster Provider Types:
  powershell  - Get cluster info from PowerShell (default, Windows)
  file        - Read cluster info from YAML/JSON file
  env         - Read cluster info from environment variables
                (HEALTH_CLUSTER_NAME, HEALTH_NODE_NAME, HEALTH_NODE_TYPE, HEALTH_NODE_HOST)
        """
    )

    parser.add_argument('--cluster', '-c',
                        help='Cluster name (for single node collection)')
    parser.add_argument('--node', '-n',
                        help='Node name (for single node collection)')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Collect metrics for all nodes from cluster provider')
    parser.add_argument('--provider', '-p',
                        choices=['powershell', 'file', 'env'],
                        default='powershell',
                        help='Cluster information provider type (default: powershell)')
    parser.add_argument('--cluster-name',
                        default='local-cluster',
                        help='Cluster name for powershell provider (default: local-cluster)')
    parser.add_argument('--clusters-config',
                        default='config/clusters.yaml',
                        help='Path to clusters configuration file (for file provider)')
    parser.add_argument('--ps-script',
                        help='Path to custom PowerShell script for cluster discovery')
    parser.add_argument('--output-dir', '-o',
                        default='data/metrics',
                        help='Output directory for metric storage')
    parser.add_argument('--stdout', action='store_true',
                        help='Output metrics to stdout as JSON')
    parser.add_argument('--metrics', '-m',
                        help='Comma-separated list of metrics to collect (default: all)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')

    args = parser.parse_args()

    # Validate arguments
    if not args.all and (not args.cluster or not args.node):
        parser.error("Either --all or both --cluster and --node are required")

    # Get collectors
    all_collectors = get_all_collectors()
    if args.metrics:
        metric_names = [m.strip() for m in args.metrics.split(',')]
        collectors = [c for c in all_collectors if c.name in metric_names]
        if not collectors:
            print(f"Error: No valid metrics found in: {args.metrics}", file=sys.stderr)
            print(f"Available metrics: {', '.join(c.name for c in all_collectors)}",
                  file=sys.stderr)
            sys.exit(1)
    else:
        collectors = all_collectors

    # Initialize storage
    storage = MetricStorage(base_dir=args.output_dir)

    # Collect metrics
    all_metrics = []

    if args.all:
        # Create cluster provider based on type
        if args.verbose:
            print(f"Using cluster provider: {args.provider}")

        try:
            provider = create_cluster_provider(
                provider_type=args.provider,
                config_path=args.clusters_config,
                script_path=args.ps_script,
                cluster_name=args.cluster_name
            )
        except Exception as e:
            print(f"Error creating cluster provider: {e}", file=sys.stderr)
            sys.exit(1)

        clusters = provider.get_clusters()
        if not clusters:
            print(f"Error: No clusters found from {args.provider} provider", file=sys.stderr)
            sys.exit(1)

        if args.verbose:
            print(f"Discovered {len(clusters)} cluster(s)")

        for cluster in clusters:
            if args.verbose:
                print(f"  Cluster: {cluster.name} ({len(cluster.nodes)} nodes)")

            for node in cluster.nodes:
                if args.verbose:
                    print(f"    Collecting metrics for {cluster.name}/{node.name}...")

                metrics = collect_metrics(node.name, cluster.name, collectors)
                all_metrics.extend(metrics)

                if not args.stdout:
                    storage.store_batch(metrics)

    else:
        # Collect for specific node
        if args.verbose:
            print(f"Collecting metrics for {args.cluster}/{args.node}...")

        metrics = collect_metrics(args.node, args.cluster, collectors)
        all_metrics.extend(metrics)

        if not args.stdout:
            storage.store_batch(metrics)

    # Output results
    if args.stdout:
        output = [m.to_dict() for m in all_metrics]
        print(json.dumps(output, indent=2))
    elif args.verbose:
        print(f"Collected {len(all_metrics)} metrics")
        for metric in all_metrics:
            print(f"  {metric.metric_name}: {metric.value}{metric.unit}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
