"""
Cluster Information Provider Module

This module provides an extensible interface for collecting cluster information
from various sources including local files, databases, and PowerShell commands.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import yaml
import json
import subprocess
import os


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

    def get_node(self, node_name: str) -> Optional[Node]:
        """Get a node by name."""
        for node in self.nodes:
            if node.name == node_name:
                return node
        return None


class ClusterInfoProvider(ABC):
    """Abstract base class for cluster information providers."""

    @abstractmethod
    def get_clusters(self) -> List[Cluster]:
        """Retrieve all clusters."""
        pass

    @abstractmethod
    def get_cluster(self, cluster_name: str) -> Optional[Cluster]:
        """Retrieve a specific cluster by name."""
        pass

    @abstractmethod
    def refresh(self) -> None:
        """Refresh cluster information from the source."""
        pass


class FileClusterProvider(ClusterInfoProvider):
    """Cluster information provider that reads from a YAML file."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self._clusters: List[Cluster] = []
        self.refresh()

    def get_clusters(self) -> List[Cluster]:
        return self._clusters

    def get_cluster(self, cluster_name: str) -> Optional[Cluster]:
        for cluster in self._clusters:
            if cluster.name == cluster_name:
                return cluster
        return None

    def refresh(self) -> None:
        self._clusters = []
        if not os.path.exists(self.file_path):
            return

        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

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


class DatabaseClusterProvider(ClusterInfoProvider):
    """Cluster information provider that reads from a database."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._clusters: List[Cluster] = []
        # Database connection would be established here
        self.refresh()

    def get_clusters(self) -> List[Cluster]:
        return self._clusters

    def get_cluster(self, cluster_name: str) -> Optional[Cluster]:
        for cluster in self._clusters:
            if cluster.name == cluster_name:
                return cluster
        return None

    def refresh(self) -> None:
        # Placeholder for database implementation
        # In a real implementation, this would query the database
        self._clusters = []


class PowerShellClusterProvider(ClusterInfoProvider):
    """Cluster information provider that uses dmclient.exe via PowerShell."""

    # CSV column indices based on machineinfo.csv format
    COL_MACHINE_NAME = 0
    COL_MACHINE_FUNCTION = 2
    COL_STATIC_IP = 6
    COL_STATUS = 10
    COL_ENVIRONMENT = 17

    def __init__(self, region: str, cluster_name: str,
                 dmclient_path: str = ".\\dmclient.exe",
                 machine_function: str = "CH"):
        """
        Initialize PowerShellClusterProvider.

        Args:
            region: The region code (e.g., "MWHE01")
            cluster_name: The cluster name (e.g., "MTTitanMetricsBE-Prod-MWHE01")
            dmclient_path: Path to dmclient.exe
            machine_function: Machine function filter (e.g., "CH")
        """
        self.region = region
        self.cluster_name = cluster_name
        self.dmclient_path = dmclient_path
        self.machine_function = machine_function
        self._clusters: List[Cluster] = []
        self.refresh()

    def get_clusters(self) -> List[Cluster]:
        return self._clusters

    def get_cluster(self, cluster_name: str) -> Optional[Cluster]:
        for cluster in self._clusters:
            if cluster.name == cluster_name:
                return cluster
        return None

    def refresh(self) -> None:
        self._clusters = []
        try:
            # Execute dmclient.exe command via PowerShell
            # Command: .\dmclient.exe -C <region> -c "GetMachineInfo -f <function> -w <cluster_name>"
            dm_command = f'GetMachineInfo -f {self.machine_function} -w {self.cluster_name}'
            result = subprocess.run(
                ['powershell', '-Command',
                 f'{self.dmclient_path} -C {self.region} -c "{dm_command}"'],
                capture_output=True,
                text=True,
                timeout=120
            )

            if result.returncode == 0 and result.stdout:
                self._parse_csv_output(result.stdout)

        except subprocess.TimeoutExpired:
            # Timeout - leave clusters empty
            pass
        except Exception:
            # Other errors - leave clusters empty
            pass

    def _parse_csv_output(self, csv_content: str) -> None:
        """Parse CSV output from dmclient.exe and extract node information."""
        nodes = self.parse_machine_info_csv(csv_content)

        if nodes:
            cluster = Cluster(
                name=self.cluster_name,
                description=f"Cluster {self.cluster_name} in region {self.region}",
                nodes=nodes
            )
            self._clusters.append(cluster)

    @classmethod
    def parse_machine_info_csv(cls, csv_content: str) -> List[Node]:
        """
        Parse machineinfo CSV content and extract nodes.

        Args:
            csv_content: The CSV content string from dmclient.exe output

        Returns:
            List of Node objects parsed from the CSV
        """
        nodes = []
        lines = csv_content.strip().split('\n')

        for line in lines:
            # Skip comment lines (starting with #) and empty lines
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            fields = line.split(',')

            # Ensure we have enough fields
            if len(fields) <= cls.COL_ENVIRONMENT:
                continue

            machine_name = fields[cls.COL_MACHINE_NAME].strip()
            machine_function = fields[cls.COL_MACHINE_FUNCTION].strip()
            static_ip = fields[cls.COL_STATIC_IP].strip()
            status = fields[cls.COL_STATUS].strip()
            environment = fields[cls.COL_ENVIRONMENT].strip()

            # Skip if machine name is empty
            if not machine_name:
                continue

            node = Node(
                name=machine_name,
                type=machine_function,
                host=static_ip if static_ip else machine_name,
                collection_method="remote",
                attributes={
                    "status": status,
                    "environment": environment
                }
            )
            nodes.append(node)

        return nodes


class ClusterProviderFactory:
    """Factory class for creating cluster information providers."""

    @staticmethod
    def create(provider_type: str, config: Dict[str, Any]) -> ClusterInfoProvider:
        """Create a cluster provider based on type and configuration."""
        if provider_type == 'file':
            return FileClusterProvider(config.get('file_path', 'config/clusters.yaml'))
        elif provider_type == 'database':
            return DatabaseClusterProvider(config.get('connection_string', ''))
        elif provider_type == 'powershell':
            return PowerShellClusterProvider(
                region=config.get('region', ''),
                cluster_name=config.get('cluster_name', ''),
                dmclient_path=config.get('dmclient_path', '.\\dmclient.exe'),
                machine_function=config.get('machine_function', 'CH')
            )
        else:
            raise ValueError(f"Unknown provider type: {provider_type}")
