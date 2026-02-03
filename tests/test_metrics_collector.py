import pytest
from src.metrics.collector import create_default_registry, MetricStorage
import tempfile
import shutil
import os

def test_default_registry_collects_metrics():
    registry = create_default_registry()
    metrics = registry.collect_all(node_name="test-node", cluster_name="test-cluster")
    assert isinstance(metrics, list)
    assert len(metrics) > 0
    for metric in metrics:
        assert hasattr(metric, 'metric_name')
        assert hasattr(metric, 'value')
        assert hasattr(metric, 'timestamp')
        assert hasattr(metric, 'node_name')
        assert hasattr(metric, 'cluster_name')

def test_metric_storage_store_and_query():
    temp_dir = tempfile.mkdtemp()
    try:
        storage = MetricStorage(base_dir=temp_dir)
        registry = create_default_registry()
        metrics = registry.collect_all(node_name="test-node", cluster_name="test-cluster")
        for metric in metrics:
            storage.store(metric)
        # Query back
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        queried = storage.query("test-cluster", "test-node", now - timedelta(hours=1), now)
        assert isinstance(queried, list)
        assert len(queried) >= len(metrics)
    finally:
        shutil.rmtree(temp_dir)
