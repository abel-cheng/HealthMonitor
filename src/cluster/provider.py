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
    """Cluster information provider that uses PowerShell commands."""

    def __init__(self, script_path: Optional[str] = None):
        self.script_path = script_path
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
            if self.script_path and os.path.exists(self.script_path):
                # Execute custom PowerShell script
                result = subprocess.run(
                    ['powershell', '-ExecutionPolicy', 'Bypass', '-File', self.script_path],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
            else:
                # Default PowerShell command to get local computer info
                ps_command = '''
                $computerInfo = Get-ComputerInfo | Select-Object CsName, CsDomain
                $result = @{
                    clusters = @(
                        @{
                            name = "local-cluster"
                            description = "Local machine cluster"
                            nodes = @(
                                @{
                                    name = $computerInfo.CsName
                                    type = "standalone"
                                    host = "localhost"
                                    collection_method = "local"
                                }
                            )
                        }
                    )
                }
                $result | ConvertTo-Json -Depth 10
                '''
                result = subprocess.run(
                    ['powershell', '-Command', ps_command],
                    capture_output=True,
                    text=True,
                    timeout=60
                )

            if result.returncode == 0 and result.stdout:
                data = json.loads(result.stdout)
                self._parse_cluster_data(data)

        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):
            # Fallback to empty clusters on error
            pass

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
            return PowerShellClusterProvider(config.get('script_path'))
        else:
            raise ValueError(f"Unknown provider type: {provider_type}")
