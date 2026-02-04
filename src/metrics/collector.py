"""
Metrics Collection Module

This module provides an extensible framework for collecting various metrics
from cluster nodes. Metrics are collected periodically and stored in JSON format.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
import json
import os
import uuid
import requests
import threading
import time


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


class MetricCollector(ABC):
    """Abstract base class for metric collectors."""

    def __init__(self, name: str, unit: str = "", interval: int = 60):
        self.name = name
        self.unit = unit
        self.interval = interval

    @abstractmethod
    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        """Collect the metric value."""
        pass

    def _create_metric(self, node_name: str, cluster_name: str, value: Any) -> MetricValue:
        """Helper method to create a MetricValue instance."""
        return MetricValue(
            metric_id=str(uuid.uuid4()),
            metric_name=self.name,
            value=value,
            timestamp=datetime.utcnow().isoformat(),
            node_name=node_name,
            cluster_name=cluster_name,
            unit=self.unit
        )


class ClickHouseUptimeCollector(MetricCollector):
    """
    Collects ClickHouse server uptime in seconds via HTTP API.
    
    Uses: SELECT uptime() query through ClickHouse HTTP interface.
    Equivalent curl: curl 'http://host:8123/?query=SELECT%20uptime()'
    """

    def __init__(self, host: str = "localhost", port: int = 8123, 
                 user: str = "default", password: str = "",
                 interval: int = 60, timeout: int = 10):
        super().__init__(name="clickhouse_uptime", unit="seconds", interval=interval)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout

    def _get_base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        """
        Collect ClickHouse uptime via HTTP query.
        Returns uptime in seconds, or -1 if unreachable.
        """
        try:
            url = f"{self._get_base_url()}/?query=SELECT%20uptime()"
            auth = (self.user, self.password) if self.password else None
            response = requests.get(url, auth=auth, timeout=self.timeout)
            response.raise_for_status()
            uptime = int(response.text.strip())
        except Exception:
            uptime = -1
        return self._create_metric(node_name, cluster_name, uptime)


class ClickHouseStatusCollector(MetricCollector):
    """
    Collects ClickHouse server health status via HTTP ping endpoint.
    
    Uses: /ping endpoint which returns 'Ok.' if server is healthy.
    Equivalent curl: curl 'http://host:8123/ping'
    
    Returns: 1 = healthy, 0 = unhealthy/unreachable
    """

    def __init__(self, host: str = "localhost", port: int = 8123,
                 user: str = "default", password: str = "",
                 interval: int = 60, timeout: int = 10):
        super().__init__(name="clickhouse_status", unit="", interval=interval)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout

    def _get_base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        """
        Check ClickHouse health via /ping endpoint.
        Returns 1 if healthy, 0 if unhealthy or unreachable.
        """
        try:
            url = f"{self._get_base_url()}/ping"
            auth = (self.user, self.password) if self.password else None
            response = requests.get(url, auth=auth, timeout=self.timeout)
            status = 1 if response.status_code == 200 and response.text.strip() == "Ok." else 0
        except Exception:
            status = 0
        return self._create_metric(node_name, cluster_name, status)


class MetricStorage:
    """Handles storage of metrics to JSON files organized by hour."""

    def __init__(self, base_dir: str = "data/metrics"):
        self.base_dir = base_dir
        self._lock = threading.Lock()
        os.makedirs(base_dir, exist_ok=True)

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

        with self._lock:
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

    def query(self, cluster_name: str, node_name: str,
              start_time: datetime, end_time: datetime,
              metric_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query stored metrics within a time range."""
        results = []
        current = start_time.replace(minute=0, second=0, microsecond=0)

        while current <= end_time:
            file_path = self._get_file_path(cluster_name, node_name, current)
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        metrics = json.load(f)
                        for m in metrics:
                            m_time = datetime.fromisoformat(m['timestamp'])
                            if start_time <= m_time <= end_time:
                                if metric_name is None or m['metric_name'] == metric_name:
                                    results.append(m)
                except (json.JSONDecodeError, IOError):
                    pass

            # Move to next hour
            current = current.replace(hour=current.hour + 1) if current.hour < 23 else \
                current.replace(day=current.day + 1, hour=0)

        return sorted(results, key=lambda x: x['timestamp'])

    def get_latest(self, cluster_name: str, node_name: str,
                   metric_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get the latest metrics for a node."""
        now = datetime.utcnow()
        # Query last hour
        start_time = now.replace(minute=0, second=0, microsecond=0)
        metrics = self.query(cluster_name, node_name, start_time, now, metric_name)

        if not metrics:
            # Try previous hour if no data in current hour
            prev_hour = start_time.replace(hour=start_time.hour - 1) if start_time.hour > 0 else \
                start_time.replace(day=start_time.day - 1, hour=23)
            metrics = self.query(cluster_name, node_name, prev_hour, start_time, metric_name)

        return metrics


class MetricRegistry:
    """Registry for managing metric collectors."""

    def __init__(self):
        self._collectors: Dict[str, MetricCollector] = {}

    def register(self, collector: MetricCollector) -> None:
        """Register a metric collector."""
        self._collectors[collector.name] = collector

    def unregister(self, name: str) -> None:
        """Unregister a metric collector."""
        if name in self._collectors:
            del self._collectors[name]

    def get_collector(self, name: str) -> Optional[MetricCollector]:
        """Get a collector by name."""
        return self._collectors.get(name)

    def get_all_collectors(self) -> List[MetricCollector]:
        """Get all registered collectors."""
        return list(self._collectors.values())

    def collect_all(self, node_name: str, cluster_name: str) -> List[MetricValue]:
        """Collect all metrics for a node."""
        metrics = []
        for collector in self._collectors.values():
            try:
                metric = collector.collect(node_name, cluster_name)
                metrics.append(metric)
            except Exception:
                # Log error but continue with other collectors
                pass
        return metrics


def create_default_registry(host: str = "localhost", port: int = 8123,
                            user: str = "admin", password: str = "") -> MetricRegistry:
    """
    Create a registry with ClickHouse metric collectors.
    
    Args:
        host: ClickHouse server hostname
        port: ClickHouse HTTP port (default 8123)
        user: ClickHouse username (default: admin)
        password: ClickHouse password
    
    Collectors:
        - clickhouse_status: Returns: 1 = healthy, 0 = unhealthy/unreachable
        - clickhouse_uptime: Returns node uptime in seconds
    
    Example curl commands:
        curl -u 'admin:password' http://host:8123/ping
        curl -u 'admin:password' 'http://host:8123/?query=SELECT%20uptime()%20FORMAT%20JSON'
    """
    registry = MetricRegistry()

    # Register ClickHouse collectors: ping + uptime
    registry.register(ClickHouseStatusCollector(host=host, port=port, user=user, password=password))
    registry.register(ClickHouseUptimeCollector(host=host, port=port, user=user, password=password))

    return registry
