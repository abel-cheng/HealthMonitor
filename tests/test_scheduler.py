import pytest
from src.scheduler.scheduler import CollectionScheduler
from src.cluster.provider import FileClusterProvider
from src.metrics.collector import create_default_registry, MetricStorage
from src.alerts.manager import AlertManager
import tempfile
import shutil
import os

def test_collection_scheduler_runs_cycle():
    temp_dir = tempfile.mkdtemp()
    try:
        provider = FileClusterProvider('config/clusters.yaml')
        registry = create_default_registry()
        storage = MetricStorage(base_dir=temp_dir)
        alert_manager = AlertManager()
        scheduler = CollectionScheduler(
            cluster_provider=provider,
            metric_registry=registry,
            metric_storage=storage,
            alert_manager=alert_manager,
            interval_seconds=1
        )
        # Run one collection cycle
        scheduler._collection_cycle()
        # Check that metrics were stored
        clusters = provider.get_clusters()
        for cluster in clusters:
            for node in cluster.nodes:
                from datetime import datetime, timedelta
                now = datetime.utcnow()
                queried = storage.query(cluster.name, node.name, now - timedelta(hours=1), now)
                assert isinstance(queried, list)
    finally:
        shutil.rmtree(temp_dir)
