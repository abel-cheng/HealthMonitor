"""
Unit tests for collector_cli module.
"""
import pytest
import os
import sys
import json
import tempfile
import shutil
from datetime import datetime
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector_cli import (
    collect_metrics_for_node,
    create_provider,
    setup_logging,
)
from src.cluster.provider import FileClusterProvider, Node
from src.metrics.collector import (
    MetricStorage,
    MetricValue,
    MetricCollector,
    get_all_collectors,
    ClickHouseStatusCollector,
)


# Test data paths
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_CLUSTERS_YAML = os.path.join(TEST_DIR, 'test_clusters.yaml')


class TestClickHouseStatusCollector:
    """Tests for ClickHouseStatusCollector."""

    def test_clickhouse_status_collector_healthy(self):
        """Test ClickHouseStatusCollector returns 1 when server responds Ok."""
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "Ok."
            mock_get.return_value = mock_response
            
            collector = ClickHouseStatusCollector(host="localhost", port=8123)
            metric = collector.collect("test-node", "test-cluster")
            
            assert metric.metric_name == "clickhouse_status"
            assert metric.value == 1
            assert metric.node_name == "test-node"
            assert metric.cluster_name == "test-cluster"

    def test_clickhouse_status_collector_unhealthy(self):
        """Test ClickHouseStatusCollector returns 0 when server is down."""
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception("Connection refused")
            
            collector = ClickHouseStatusCollector(host="localhost", port=8123)
            metric = collector.collect("test-node", "test-cluster")
            
            assert metric.metric_name == "clickhouse_status"
            assert metric.value == 0

    def test_get_all_collectors(self):
        """Test get_all_collectors returns ClickHouseStatusCollector."""
        collectors = get_all_collectors(host="localhost", port=8123)
        
        assert len(collectors) == 1
        assert collectors[0].name == "clickhouse_status"
        assert isinstance(collectors[0], ClickHouseStatusCollector)


class TestFileClusterProvider:
    """Tests for FileClusterProvider with test_clusters.yaml."""

    def test_load_test_cluster(self):
        """Test loading test cluster from YAML file."""
        provider = FileClusterProvider(TEST_CLUSTERS_YAML)
        clusters = provider.get_clusters()
        
        assert len(clusters) == 1
        cluster = clusters[0]
        assert cluster.name == "test-cluster"
        assert cluster.description == "Test cluster for unit testing"

    def test_test_cluster_has_one_node(self):
        """Test that test cluster has exactly one node."""
        provider = FileClusterProvider(TEST_CLUSTERS_YAML)
        cluster = provider.get_cluster("test-cluster")
        
        assert cluster is not None
        assert len(cluster.nodes) == 1

    def test_test_cluster_node_details(self):
        """Test test cluster node has correct details."""
        provider = FileClusterProvider(TEST_CLUSTERS_YAML)
        cluster = provider.get_cluster("test-cluster")
        node = cluster.nodes[0]
        
        assert node.name == "test-node-01"
        assert node.host == "13.66.204.72"
        assert node.type == "worker"
        assert node.collection_method == "remote"
        assert node.attributes.get("environment") == "test"


class TestCollectMetricsForNode:
    """Tests for collect_metrics_for_node function."""

    def test_collect_metrics_for_test_node(self):
        """Test collecting clickhouse_status metric for test node."""
        provider = FileClusterProvider(TEST_CLUSTERS_YAML)
        cluster = provider.get_cluster("test-cluster")
        node = cluster.nodes[0]
        
        logger = setup_logging(verbose=False)
        
        # Mock the ClickHouse request
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "Ok."
            mock_get.return_value = mock_response
            
            metrics = collect_metrics_for_node(node, cluster.name, 8123, logger)
            
            assert len(metrics) == 1
            assert metrics[0].metric_name == "clickhouse_status"
            assert metrics[0].node_name == "test-node-01"
            assert metrics[0].cluster_name == "test-cluster"
            assert metrics[0].value == 1

    def test_collect_metrics_for_unhealthy_node(self):
        """Test collecting metrics when ClickHouse is down."""
        provider = FileClusterProvider(TEST_CLUSTERS_YAML)
        cluster = provider.get_cluster("test-cluster")
        node = cluster.nodes[0]
        
        logger = setup_logging(verbose=False)
        
        # Mock connection failure
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception("Connection refused")
            
            metrics = collect_metrics_for_node(node, cluster.name, 8123, logger)
            
            assert len(metrics) == 1
            assert metrics[0].metric_name == "clickhouse_status"
            assert metrics[0].value == 0


class TestCreateProvider:
    """Tests for create_provider function."""

    def test_create_file_provider(self):
        """Test creating file provider."""
        logger = setup_logging(verbose=False)
        config = {'config_path': TEST_CLUSTERS_YAML}
        
        provider = create_provider('file', 'test-cluster', config, logger)
        
        assert provider is not None
        clusters = provider.get_clusters()
        assert len(clusters) == 1
        assert clusters[0].name == "test-cluster"

    def test_create_provider_with_invalid_type(self):
        """Test creating provider with invalid type returns None."""
        logger = setup_logging(verbose=False)
        config = {}
        
        provider = create_provider('invalid', 'test-cluster', config, logger)
        
        assert provider is None


class TestMetricStorage:
    """Tests for MetricStorage integration."""

    def test_store_and_retrieve_metrics(self):
        """Test storing and retrieving metrics."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = MetricStorage(base_dir=temp_dir)
            
            # Create a test metric
            metric = MetricValue(
                metric_id="test-id-001",
                metric_name="test_metric",
                value=42.5,
                timestamp=datetime.utcnow().isoformat(),
                node_name="test-node-01",
                cluster_name="test-cluster",
                unit="%",
                tags={"host": "13.66.204.72"}
            )
            
            # Store the metric
            storage.store(metric)
            
            # Verify file was created
            # File structure: base_dir/cluster_name/node_name/YYYY/MM/DD/HH.log
            now = datetime.utcnow()
            expected_dir = os.path.join(
                temp_dir, "test-cluster", "test-node-01",
                now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
            )
            expected_file = os.path.join(expected_dir, now.strftime("%H") + ".log")
            
            assert os.path.exists(expected_file)
            
            # Verify content - now CSV format with only 3 columns
            with open(expected_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # First line is header, second line is data
            assert len(lines) == 2
            assert lines[0].strip() == "# metric_name,timestamp,value"
            
            # Parse data line - only 3 columns now
            data_parts = lines[1].strip().split(',')
            assert len(data_parts) == 3
            assert data_parts[0] == "test_metric"
            assert float(data_parts[2]) == 42.5

    def test_store_batch_metrics(self):
        """Test storing multiple metrics in batch."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = MetricStorage(base_dir=temp_dir)
            
            timestamp = datetime.utcnow().isoformat()
            metrics = [
                MetricValue(
                    metric_id=f"test-id-{i}",
                    metric_name=f"metric_{i}",
                    value=i * 10,
                    timestamp=timestamp,
                    node_name="test-node-01",
                    cluster_name="test-cluster",
                    unit="",
                    tags={"host": "13.66.204.72"}
                )
                for i in range(5)
            ]
            
            # Store batch
            storage.store_batch(metrics)
            
            # Query and verify
            now = datetime.utcnow()
            start_time = now.replace(minute=0, second=0, microsecond=0)
            end_time = now
            
            results = storage.query("test-cluster", "test-node-01", start_time, end_time)
            
            assert len(results) == 5


class TestCollectorCLIIntegration:
    """Integration tests for collector CLI."""

    def test_full_collection_workflow(self):
        """Test full workflow: load cluster -> collect metrics -> store."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup
            logger = setup_logging(verbose=False)
            provider = FileClusterProvider(TEST_CLUSTERS_YAML)
            storage = MetricStorage(base_dir=temp_dir)
            
            # Get cluster and node
            cluster = provider.get_cluster("test-cluster")
            assert cluster is not None
            
            # Mock ClickHouse request
            with patch('requests.get') as mock_get:
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.text = "Ok."
                mock_get.return_value = mock_response
                
                # Collect metrics for each node
                all_metrics = []
                for node in cluster.nodes:
                    metrics = collect_metrics_for_node(node, cluster.name, 8123, logger)
                    all_metrics.extend(metrics)
                    storage.store_batch(metrics)
                
                # Verify
                assert len(all_metrics) == 1  # 1 collector * 1 node
                
                # Check storage
                now = datetime.utcnow()
                start_time = now.replace(minute=0, second=0, microsecond=0)
                stored_metrics = storage.query("test-cluster", "test-node-01", start_time, now)
                
                assert len(stored_metrics) == 1
                assert stored_metrics[0]["metric_name"] == "clickhouse_status"
                assert stored_metrics[0]["value"] == 1
