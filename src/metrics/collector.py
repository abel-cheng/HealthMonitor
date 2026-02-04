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


class ClickHouseStatusCollector(MetricCollector):
    """
    Collects ClickHouse server health status via HTTP ping endpoint.
    
    Uses: /ping endpoint which returns 'Ok.' if server is healthy.
    Equivalent curl: curl 'http://host:8123/ping'
    
    Returns: 1 = healthy, 0 = unhealthy/unreachable
    """

    def __init__(self, host: str = "localhost", port: int = 8123,
                 interval: int = 60, timeout: int = 10):
        super().__init__(name="clickhouse_status", unit="", interval=interval)
        self.host = host
        self.port = port
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
            response = requests.get(url, timeout=self.timeout)
            status = 1 if response.status_code == 200 and response.text.strip() == "Ok." else 0
        except Exception:
            status = 0
        return self._create_metric(node_name, cluster_name, status)


def get_all_collectors(host: str = "localhost", port: int = 8123) -> List[MetricCollector]:
    """
    Get all available metric collectors.
    
    Args:
        host: ClickHouse host address
        port: ClickHouse HTTP port
    
    Returns:
        List of metric collectors (currently only ClickHouseStatusCollector)
    """
    return [
        ClickHouseStatusCollector(host=host, port=port),
    ]


# =============================================================================
# DEPRECATED: Local System Metric Collectors (using psutil)
# These collectors are deprecated and will be removed in a future version.
# Use ClickHouseStatusCollector for monitoring ClickHouse health.
# =============================================================================

import warnings

# Try to import psutil, handle if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


def _deprecated_warning(class_name: str):
    """Emit deprecation warning for old collectors."""
    warnings.warn(
        f"{class_name} is deprecated and will be removed in a future version. "
        "Use ClickHouseStatusCollector instead.",
        DeprecationWarning,
        stacklevel=3
    )


class CPUPercentCollector(MetricCollector):
    """
    DEPRECATED: Collects CPU usage percentage.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, interval: int = 60):
        _deprecated_warning("CPUPercentCollector")
        super().__init__(name="cpu_percent", unit="%", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.cpu_percent(interval=1)
        else:
            value = 0.0
        return self._create_metric(node_name, cluster_name, value)


class MemoryPercentCollector(MetricCollector):
    """
    DEPRECATED: Collects memory usage percentage.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, interval: int = 60):
        _deprecated_warning("MemoryPercentCollector")
        super().__init__(name="memory_percent", unit="%", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.virtual_memory().percent
        else:
            value = 0.0
        return self._create_metric(node_name, cluster_name, value)


class MemoryUsedCollector(MetricCollector):
    """
    DEPRECATED: Collects memory used in bytes.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, interval: int = 60):
        _deprecated_warning("MemoryUsedCollector")
        super().__init__(name="memory_used", unit="bytes", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.virtual_memory().used
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class DiskPercentCollector(MetricCollector):
    """
    DEPRECATED: Collects disk usage percentage.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, path: str = None, interval: int = 60):
        _deprecated_warning("DiskPercentCollector")
        super().__init__(name="disk_percent", unit="%", interval=interval)
        self.path = path or ("C:\\" if os.name == 'nt' else "/")

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        if PSUTIL_AVAILABLE:
            try:
                value = psutil.disk_usage(self.path).percent
            except Exception:
                value = 0.0
        else:
            value = 0.0
        return self._create_metric(node_name, cluster_name, value)


class DiskUsedCollector(MetricCollector):
    """
    DEPRECATED: Collects disk used in bytes.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, path: str = None, interval: int = 60):
        _deprecated_warning("DiskUsedCollector")
        super().__init__(name="disk_used", unit="bytes", interval=interval)
        self.path = path or ("C:\\" if os.name == 'nt' else "/")

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        if PSUTIL_AVAILABLE:
            try:
                value = psutil.disk_usage(self.path).used
            except Exception:
                value = 0
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class NetworkBytesRecvCollector(MetricCollector):
    """
    DEPRECATED: Collects network bytes received.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, interval: int = 60):
        _deprecated_warning("NetworkBytesRecvCollector")
        super().__init__(name="network_bytes_recv", unit="bytes", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.net_io_counters().bytes_recv
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class NetworkBytesSentCollector(MetricCollector):
    """
    DEPRECATED: Collects network bytes sent.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, interval: int = 60):
        _deprecated_warning("NetworkBytesSentCollector")
        super().__init__(name="network_bytes_sent", unit="bytes", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = psutil.net_io_counters().bytes_sent
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class NodeStatusCollector(MetricCollector):
    """
    DEPRECATED: Collects node status (1 = up, 0 = down).
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, interval: int = 60):
        _deprecated_warning("NodeStatusCollector")
        super().__init__(name="node_status", unit="", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        # Node is up if this collector runs successfully
        return self._create_metric(node_name, cluster_name, 1)


class LoadAverageCollector(MetricCollector):
    """
    DEPRECATED: Collects system load average.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, interval: int = 60):
        _deprecated_warning("LoadAverageCollector")
        super().__init__(name="load_average", unit="", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
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
    """
    DEPRECATED: Collects number of running processes.
    This collector is deprecated. Use ClickHouseStatusCollector instead.
    """

    def __init__(self, interval: int = 60):
        _deprecated_warning("ProcessCountCollector")
        super().__init__(name="process_count", unit="", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        if PSUTIL_AVAILABLE:
            value = len(psutil.pids())
        else:
            value = 0
        return self._create_metric(node_name, cluster_name, value)


class MetricStorage:
    """Handles storage of metrics to CSV-style log files organized by directory hierarchy."""

    # CSV header format - only 3 columns: metric_name, timestamp, value
    CSV_HEADER = "# metric_name,timestamp,value"

    def __init__(self, base_dir: str = "data/metrics"):
        self.base_dir = base_dir
        self._lock = threading.Lock()
        os.makedirs(base_dir, exist_ok=True)

    def _get_file_path(self, metric: MetricValue, timestamp: datetime) -> str:
        """
        Generate file path with all context in directory structure.
        Format: base_dir/cluster_name/node_name/YYYY/MM/DD/HH.log
        """
        date_dir = timestamp.strftime("%Y/%m/%d")
        hour_file = timestamp.strftime("%H") + ".log"
        dir_path = os.path.join(
            self.base_dir, 
            metric.cluster_name, 
            metric.node_name, 
            date_dir
        )
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, hour_file)

    def _metric_to_csv_line(self, metric: MetricValue) -> str:
        """Convert a metric to CSV line format (only 3 columns)."""
        return f"{metric.metric_name},{metric.timestamp},{metric.value}"

    def _csv_line_to_metric(self, line: str, cluster_name: str, node_name: str) -> Optional[Dict[str, Any]]:
        """Parse a CSV line back to metric dict, with context from directory path."""
        line = line.strip()
        if not line or line.startswith('#'):
            return None
        
        parts = line.split(',', 2)  # Split into at most 3 parts
        if len(parts) < 3:
            return None
        
        try:
            value_str = parts[2]
            value = float(value_str) if '.' in value_str else int(value_str)
            
            return {
                'metric_name': parts[0],
                'timestamp': parts[1],
                'value': value,
                'node_name': node_name,
                'cluster_name': cluster_name,
            }
        except (ValueError, IndexError):
            return None

    def store(self, metric: MetricValue) -> None:
        """Store a single metric value (append to log file)."""
        timestamp = datetime.fromisoformat(metric.timestamp)
        file_path = self._get_file_path(metric, timestamp)

        with self._lock:
            # Check if file exists and has header
            file_exists = os.path.exists(file_path)
            
            with open(file_path, 'a', encoding='utf-8') as f:
                if not file_exists:
                    f.write(self.CSV_HEADER + '\n')
                f.write(self._metric_to_csv_line(metric) + '\n')

    def store_batch(self, metrics: List[MetricValue]) -> None:
        """Store multiple metrics efficiently."""
        for metric in metrics:
            self.store(metric)


class JsonMetricStorage:
    """
    Handles storage of metrics to JSON files.
    
    Directory structure: <base_dir>/<cluster>/<year>/<month>/<day>/ServceLogs_<timestamp>.json
    JSON format: Array of {clustername, machinename, metricname, metricvalue, logtime}
    
    每次采集生成一个新文件（不增量追加）。
    """
    
    # Metric ID 定义
    METRIC_ID_PING = "ch_ping"
    
    # 默认日志根目录
    DEFAULT_LOG_ROOT = r"D:\ServiceHealthMatrixLogs"

    def __init__(self, base_dir: str = None):
        self.base_dir = base_dir or self.DEFAULT_LOG_ROOT
        self._lock = threading.Lock()

    def _format_metric_json(self, metric: MetricValue, metric_id: str = None) -> dict:
        """格式化为 JSON 对象"""
        return {
            "clustername": metric.cluster_name,
            "machinename": metric.node_name,
            "metricname": metric_id or self.METRIC_ID_PING,
            "metricvalue": metric.value,
            "logtime": metric.timestamp[:19]  # YYYY-MM-DDTHH:MM:SS
        }

    def _get_file_path(self, cluster_name: str, timestamp: datetime = None) -> str:
        """
        Generate JSON file path.
        Format: <base_dir>/<cluster>/<year>/<month>/<day>/ServceLogs_<timestamp>.json
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")
        time_str = timestamp.strftime("%Y%m%d%H%M")
        
        # 目录结构: <base_dir>/<cluster>/<year>/<month>/<day>/
        date_dir = os.path.join(self.base_dir, cluster_name, year, month, day)
        os.makedirs(date_dir, exist_ok=True)
        
        # 文件名: ServceLogs_<timestamp>.json
        return os.path.join(date_dir, f"ServceLogs_{time_str}.json")

    def store_batch(self, metrics: List[MetricValue], metric_id: str = None) -> str:
        """
        Store multiple metrics to a single JSON file.
        
        Args:
            metrics: List of MetricValue objects
            metric_id: Optional metric ID (default: ch_ping)
        
        Returns:
            Path to the saved JSON file
        """
        if not metrics:
            return None
        
        # 使用第一个 metric 的 cluster_name
        cluster_name = metrics[0].cluster_name
        now = datetime.utcnow()
        json_file = self._get_file_path(cluster_name, now)
        
        # 构建 JSON 数据
        json_data = [self._format_metric_json(m, metric_id) for m in metrics]
        
        with self._lock:
            # 写入 JSON 文件（覆盖写入，不增量追加）
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        return json_file

    def store(self, metric: MetricValue, metric_id: str = None) -> str:
        """Store a single metric value to JSON file."""
        return self.store_batch([metric], metric_id)

    def _get_query_file_path(self, cluster_name: str, node_name: str, timestamp: datetime) -> str:
        """Generate file path for querying based on cluster, node, and hour."""
        date_dir = timestamp.strftime("%Y/%m/%d")
        hour_file = timestamp.strftime("%H") + ".log"
        return os.path.join(self.base_dir, cluster_name, node_name, date_dir, hour_file)

    def query(self, cluster_name: str, node_name: str,
              start_time: datetime, end_time: datetime,
              metric_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query stored metrics within a time range."""
        results = []
        current = start_time.replace(minute=0, second=0, microsecond=0)

        while current <= end_time:
            file_path = self._get_query_file_path(cluster_name, node_name, current)
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            m = self._csv_line_to_metric(line, cluster_name, node_name)
                            if m is None:
                                continue
                            m_time = datetime.fromisoformat(m['timestamp'])
                            if start_time <= m_time <= end_time:
                                if metric_name is None or m['metric_name'] == metric_name:
                                    results.append(m)
                except IOError:
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

    def get_health_timeline(self, cluster_name: str, node_name: str,
                            start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """
        获取节点健康状态的时间序列，展示状态变化。
        
        Args:
            cluster_name: 集群名称
            node_name: 节点名称
            start_time: 开始时间
            end_time: 结束时间
        
        Returns:
            时间序列列表，包含时间戳、状态值、以及状态变化标记
        """
        metrics = self.query(cluster_name, node_name, start_time, end_time, "clickhouse_status")
        
        timeline = []
        prev_status = None
        
        for m in metrics:
            status = m['value']
            changed = prev_status is not None and status != prev_status
            
            timeline.append({
                'timestamp': m['timestamp'],
                'status': status,
                'status_text': '健康' if status == 1 else '离线',
                'changed': changed,
                'change_type': None if not changed else ('恢复' if status == 1 else '故障')
            })
            prev_status = status
        
        return timeline

    def get_health_summary(self, cluster_name: str, node_name: str,
                           start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        获取节点健康状态摘要，包括在线/离线时间段统计。
        
        Returns:
            包含健康状态统计的字典
        """
        timeline = self.get_health_timeline(cluster_name, node_name, start_time, end_time)
        
        if not timeline:
            return {
                'node_name': node_name,
                'cluster_name': cluster_name,
                'total_checks': 0,
                'healthy_count': 0,
                'unhealthy_count': 0,
                'availability_percent': 0,
                'status_changes': [],
                'current_status': None
            }
        
        healthy_count = sum(1 for t in timeline if t['status'] == 1)
        unhealthy_count = len(timeline) - healthy_count
        
        # 记录状态变化点
        status_changes = [t for t in timeline if t['changed']]
        
        return {
            'node_name': node_name,
            'cluster_name': cluster_name,
            'total_checks': len(timeline),
            'healthy_count': healthy_count,
            'unhealthy_count': unhealthy_count,
            'availability_percent': round(healthy_count / len(timeline) * 100, 2) if timeline else 0,
            'status_changes': status_changes,
            'current_status': timeline[-1]['status_text'] if timeline else None,
            'first_check': timeline[0]['timestamp'] if timeline else None,
            'last_check': timeline[-1]['timestamp'] if timeline else None
        }

    def list_clusters(self) -> List[str]:
        """列出所有集群名称。"""
        clusters = []
        if os.path.exists(self.base_dir):
            for name in os.listdir(self.base_dir):
                if os.path.isdir(os.path.join(self.base_dir, name)):
                    clusters.append(name)
        return clusters

    def list_nodes(self, cluster_name: str) -> List[str]:
        """列出集群中所有节点。"""
        nodes = []
        cluster_dir = os.path.join(self.base_dir, cluster_name)
        if os.path.exists(cluster_dir):
            for name in os.listdir(cluster_dir):
                if os.path.isdir(os.path.join(cluster_dir, name)):
                    nodes.append(name)
        return nodes


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


def create_default_registry(host: str = "localhost", port: int = 8123) -> MetricRegistry:
    """
    Create a registry with ClickHouse ping collector.
    
    Args:
        host: ClickHouse server hostname
        port: ClickHouse HTTP port (default 8123)
    
    Collectors:
        - clickhouse_status: Ping检测 (1=健康, 0=离线)
    """
    registry = MetricRegistry()
    registry.register(ClickHouseStatusCollector(host=host, port=port))
    return registry
