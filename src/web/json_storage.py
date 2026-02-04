"""
JSON Metric Storage Reader Module

This module provides functionality to read and query metric logs stored in JSON format.
JSON format: Array of {clustername, machinename, metricname, metricvalue, logtime}
Directory structure: <base_dir>/<cluster>/<year>/<month>/<day>/ServceLogs_<timestamp>.json
"""

import os
import json
import glob
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class NodeStatus:
    """Represents the status of a node."""
    name: str
    cluster_name: str
    status: int  # 1 = healthy, 0 = down
    last_check: str
    
    @property
    def is_healthy(self) -> bool:
        return self.status == 1
    
    @property
    def status_text(self) -> str:
        return "healthy" if self.status == 1 else "down"
    
    @property
    def status_color(self) -> str:
        return "green" if self.status == 1 else "red"


@dataclass 
class ClusterStatus:
    """Represents the aggregated status of a cluster."""
    name: str
    total_nodes: int
    healthy_nodes: int
    down_nodes: int
    last_check: str
    nodes: List[NodeStatus]
    
    @property
    def status(self) -> str:
        """
        Determine cluster status:
        - green: All nodes healthy
        - yellow: 1-2 nodes down (unstable)
        - red: More than 2 nodes down (critical)
        """
        if self.down_nodes == 0:
            return "healthy"
        elif self.down_nodes <= 2:
            return "unstable"
        else:
            return "critical"
    
    @property
    def status_color(self) -> str:
        status = self.status
        if status == "healthy":
            return "green"
        elif status == "unstable":
            return "yellow"
        else:
            return "red"


class JsonMetricReader:
    """
    Reader for JSON metric log files.
    
    Directory structure: <base_dir>/<cluster>/<year>/<month>/<day>/ServceLogs_<timestamp>.json
    JSON format: [{"clustername": "", "machinename": "", "metricname": "", "metricvalue": 0/1, "logtime": ""}]
    """
    
    def __init__(self, base_dir: str):
        """
        Initialize the JSON metric reader.
        
        Args:
            base_dir: Base directory containing metric logs
        """
        self.base_dir = base_dir
    
    def list_clusters(self) -> List[str]:
        """List all cluster names in the base directory."""
        clusters = []
        if os.path.exists(self.base_dir):
            for name in os.listdir(self.base_dir):
                cluster_path = os.path.join(self.base_dir, name)
                if os.path.isdir(cluster_path):
                    # Check if it contains year directories (YYYY format)
                    for sub in os.listdir(cluster_path):
                        if sub.isdigit() and len(sub) == 4:
                            clusters.append(name)
                            break
        return sorted(clusters)
    
    def _find_json_files(self, cluster_name: str, 
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None) -> List[str]:
        """
        Find all JSON files for a cluster within a time range.
        
        Args:
            cluster_name: Name of the cluster
            start_time: Start of time range (default: 24 hours ago)
            end_time: End of time range (default: now)
        
        Returns:
            List of JSON file paths sorted by timestamp
        """
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=24)
        
        cluster_dir = os.path.join(self.base_dir, cluster_name)
        if not os.path.exists(cluster_dir):
            return []
        
        json_files = []
        
        # Iterate through date range
        current_date = start_time.date()
        end_date = end_time.date()
        
        while current_date <= end_date:
            year = str(current_date.year)
            month = f"{current_date.month:02d}"
            day = f"{current_date.day:02d}"
            
            day_dir = os.path.join(cluster_dir, year, month, day)
            if os.path.exists(day_dir):
                pattern = os.path.join(day_dir, "ServceLogs_*.json")
                for file_path in glob.glob(pattern):
                    # Extract timestamp from filename
                    filename = os.path.basename(file_path)
                    try:
                        timestamp_str = filename.replace("ServceLogs_", "").replace(".json", "")
                        file_time = datetime.strptime(timestamp_str, "%Y%m%d%H%M")
                        if start_time <= file_time <= end_time:
                            json_files.append((file_time, file_path))
                    except ValueError:
                        continue
            
            current_date += timedelta(days=1)
        
        # Sort by timestamp and return file paths
        json_files.sort(key=lambda x: x[0])
        return [f[1] for f in json_files]
    
    def _read_json_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Read and parse a JSON metric file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error reading {file_path}: {e}")
            return []
    
    def _find_latest_json_file(self, cluster_name: str) -> Optional[str]:
        """
        Find the most recent JSON file for a cluster, regardless of time range.
        This is useful when no files are found within the default time window.
        
        Args:
            cluster_name: Name of the cluster
        
        Returns:
            Path to the most recent JSON file, or None if not found
        """
        cluster_dir = os.path.join(self.base_dir, cluster_name)
        if not os.path.exists(cluster_dir):
            return None
        
        # Find all JSON files and sort by name (which includes timestamp)
        pattern = os.path.join(cluster_dir, "**", "ServceLogs_*.json")
        json_files = glob.glob(pattern, recursive=True)
        
        if not json_files:
            return None
        
        # Sort by filename (timestamp) descending and return the latest
        json_files.sort(reverse=True)
        return json_files[0]
    
    def get_latest_metrics(self, cluster_name: str) -> List[Dict[str, Any]]:
        """
        Get the latest metrics for a cluster.
        
        Args:
            cluster_name: Name of the cluster
        
        Returns:
            List of metric records from the most recent JSON file
        """
        # First try to find files within the default time window (24 hours)
        json_files = self._find_json_files(cluster_name)
        
        if json_files:
            return self._read_json_file(json_files[-1])
        
        # If no files found in time window, find the most recent file regardless of time
        latest_file = self._find_latest_json_file(cluster_name)
        if latest_file:
            return self._read_json_file(latest_file)
        
        return []
    
    def get_cluster_status(self, cluster_name: str) -> Optional[ClusterStatus]:
        """
        Get the current status of a cluster based on latest metrics.
        
        Args:
            cluster_name: Name of the cluster
        
        Returns:
            ClusterStatus object or None if no data
        """
        metrics = self.get_latest_metrics(cluster_name)
        if not metrics:
            return None
        
        # Group by machine name and get latest status
        node_statuses = {}
        last_check = None
        
        for m in metrics:
            machine = m.get('machinename')
            if not machine:
                continue
            
            status = m.get('metricvalue', 0)
            logtime = m.get('logtime', '')
            
            if machine not in node_statuses or logtime > node_statuses[machine].last_check:
                node_statuses[machine] = NodeStatus(
                    name=machine,
                    cluster_name=cluster_name,
                    status=status,
                    last_check=logtime
                )
            
            if last_check is None or logtime > last_check:
                last_check = logtime
        
        nodes = list(node_statuses.values())
        healthy = sum(1 for n in nodes if n.is_healthy)
        down = len(nodes) - healthy
        
        return ClusterStatus(
            name=cluster_name,
            total_nodes=len(nodes),
            healthy_nodes=healthy,
            down_nodes=down,
            last_check=last_check or '',
            nodes=sorted(nodes, key=lambda x: x.name)
        )
    
    def get_all_clusters_status(self) -> List[ClusterStatus]:
        """
        Get status for all clusters.
        
        Returns:
            List of ClusterStatus objects
        """
        clusters = self.list_clusters()
        statuses = []
        
        for cluster_name in clusters:
            status = self.get_cluster_status(cluster_name)
            if status:
                statuses.append(status)
        
        return statuses
    
    def _find_all_json_files(self, cluster_name: str) -> List[str]:
        """
        Find all JSON files for a cluster, regardless of time range.
        
        Args:
            cluster_name: Name of the cluster
        
        Returns:
            List of JSON file paths sorted by timestamp (oldest first)
        """
        cluster_dir = os.path.join(self.base_dir, cluster_name)
        if not os.path.exists(cluster_dir):
            return []
        
        pattern = os.path.join(cluster_dir, "**", "ServceLogs_*.json")
        json_files = glob.glob(pattern, recursive=True)
        
        # Sort by filename (which includes timestamp)
        json_files.sort()
        return json_files
    
    def get_node_history(self, cluster_name: str, node_name: str,
                         hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get historical metrics for a specific node.
        
        Args:
            cluster_name: Name of the cluster
            node_name: Name of the node
            hours: Number of hours to look back
        
        Returns:
            List of metric records sorted by time
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        # First try to find files within the time window
        json_files = self._find_json_files(cluster_name, start_time, end_time)
        
        # If no files found in time window, use all available files
        if not json_files:
            json_files = self._find_all_json_files(cluster_name)
        
        history = []
        for file_path in json_files:
            metrics = self._read_json_file(file_path)
            for m in metrics:
                if m.get('machinename') == node_name:
                    history.append({
                        'timestamp': m.get('logtime', ''),
                        'value': m.get('metricvalue', 0),
                        'metric_name': m.get('metricname', 'ch_ping'),
                        'status_text': 'healthy' if m.get('metricvalue', 0) == 1 else 'down'
                    })
        
        # Sort by timestamp and remove duplicates
        history.sort(key=lambda x: x['timestamp'])
        
        return history
    
    def get_node_status(self, cluster_name: str, node_name: str) -> Optional[NodeStatus]:
        """
        Get the current status of a specific node.
        
        Args:
            cluster_name: Name of the cluster
            node_name: Name of the node
        
        Returns:
            NodeStatus object or None if not found
        """
        cluster_status = self.get_cluster_status(cluster_name)
        if not cluster_status:
            return None
        
        for node in cluster_status.nodes:
            if node.name == node_name:
                return node
        
        return None
    
    def get_cluster_nodes(self, cluster_name: str) -> List[NodeStatus]:
        """
        Get all nodes in a cluster with their current status.
        
        Args:
            cluster_name: Name of the cluster
        
        Returns:
            List of NodeStatus objects
        """
        cluster_status = self.get_cluster_status(cluster_name)
        if not cluster_status:
            return []
        
        return cluster_status.nodes
