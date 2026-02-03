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
import psutil
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


class CPUPercentCollector(MetricCollector):
    """Collects CPU usage percentage."""

    def __init__(self, interval: int = 60):
        super().__init__(name="cpu_percent", unit="%", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        value = psutil.cpu_percent(interval=1)
        return self._create_metric(node_name, cluster_name, value)


class MemoryPercentCollector(MetricCollector):
    """Collects memory usage percentage."""

    def __init__(self, interval: int = 60):
        super().__init__(name="memory_percent", unit="%", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        memory = psutil.virtual_memory()
        return self._create_metric(node_name, cluster_name, memory.percent)


class MemoryUsedCollector(MetricCollector):
    """Collects used memory in bytes."""

    def __init__(self, interval: int = 60):
        super().__init__(name="memory_used", unit="bytes", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        memory = psutil.virtual_memory()
        return self._create_metric(node_name, cluster_name, memory.used)


class DiskPercentCollector(MetricCollector):
    """Collects disk usage percentage."""

    def __init__(self, interval: int = 60, path: str = "/"):
        super().__init__(name="disk_percent", unit="%", interval=interval)
        self.path = path

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        try:
            disk = psutil.disk_usage(self.path)
            value = disk.percent
        except Exception:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class DiskUsedCollector(MetricCollector):
    """Collects used disk space in bytes."""

    def __init__(self, interval: int = 60, path: str = "/"):
        super().__init__(name="disk_used", unit="bytes", interval=interval)
        self.path = path

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        try:
            disk = psutil.disk_usage(self.path)
            value = disk.used
        except Exception:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class NetworkBytesRecvCollector(MetricCollector):
    """Collects network bytes received."""

    def __init__(self, interval: int = 60):
        super().__init__(name="network_bytes_recv", unit="bytes", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        net_io = psutil.net_io_counters()
        return self._create_metric(node_name, cluster_name, net_io.bytes_recv)


class NetworkBytesSentCollector(MetricCollector):
    """Collects network bytes sent."""

    def __init__(self, interval: int = 60):
        super().__init__(name="network_bytes_sent", unit="bytes", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        net_io = psutil.net_io_counters()
        return self._create_metric(node_name, cluster_name, net_io.bytes_sent)


class NodeStatusCollector(MetricCollector):
    """Collects node availability status (1 = up, 0 = down)."""

    def __init__(self, interval: int = 60):
        super().__init__(name="node_status", unit="", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        # For local collection, node is always up
        # Remote collection would implement actual health check
        return self._create_metric(node_name, cluster_name, 1)


class LoadAverageCollector(MetricCollector):
    """Collects system load average (1 minute)."""

    def __init__(self, interval: int = 60):
        super().__init__(name="load_average", unit="", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        try:
            load_avg = psutil.getloadavg()[0]
        except (AttributeError, OSError):
            # getloadavg not available on Windows
            load_avg = psutil.cpu_percent() / 100.0 * psutil.cpu_count()
        return self._create_metric(node_name, cluster_name, load_avg)


class ProcessCountCollector(MetricCollector):
    """Collects number of running processes."""

    def __init__(self, interval: int = 60):
        super().__init__(name="process_count", unit="", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        count = len(psutil.pids())
        return self._create_metric(node_name, cluster_name, count)


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


def create_default_registry() -> MetricRegistry:
    """Create a registry with default metric collectors."""
    registry = MetricRegistry()

    # Register default collectors
    registry.register(CPUPercentCollector())
    registry.register(MemoryPercentCollector())
    registry.register(MemoryUsedCollector())
    registry.register(DiskPercentCollector(path="C:\\" if os.name == 'nt' else "/"))
    registry.register(DiskUsedCollector(path="C:\\" if os.name == 'nt' else "/"))
    registry.register(NetworkBytesRecvCollector())
    registry.register(NetworkBytesSentCollector())
    registry.register(NodeStatusCollector())
    registry.register(LoadAverageCollector())
    registry.register(ProcessCountCollector())

    return registry
