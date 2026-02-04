import pytest
import os
from src.cluster.provider import (
    FileClusterProvider,
    PowerShellClusterProvider,
    Cluster,
    Node
)

TEST_CLUSTERS_YAML = os.path.join(os.path.dirname(__file__), '../config/clusters.yaml')
TEST_MACHINEINFO_CSV = os.path.join(os.path.dirname(__file__), 'machineinfo.csv')


class TestFileClusterProvider:
    """Tests for FileClusterProvider."""

    def test_loads_clusters(self):
        provider = FileClusterProvider(TEST_CLUSTERS_YAML)
        clusters = provider.get_clusters()
        assert isinstance(clusters, list)
        assert len(clusters) > 0
        for cluster in clusters:
            assert isinstance(cluster, Cluster)
            assert isinstance(cluster.nodes, list)
            for node in cluster.nodes:
                assert isinstance(node, Node)

    def test_get_cluster_by_name(self):
        provider = FileClusterProvider(TEST_CLUSTERS_YAML)
        clusters = provider.get_clusters()
        if clusters:
            cluster = provider.get_cluster(clusters[0].name)
            assert cluster is not None
            assert cluster.name == clusters[0].name

    def test_get_nonexistent_cluster_returns_none(self):
        provider = FileClusterProvider(TEST_CLUSTERS_YAML)
        cluster = provider.get_cluster("nonexistent-cluster-xyz")
        assert cluster is None


class TestPowerShellClusterProvider:
    """Tests for PowerShellClusterProvider CSV parsing."""

    def test_parse_machine_info_csv_from_file(self):
        """Test parsing actual machineinfo.csv file."""
        with open(TEST_MACHINEINFO_CSV, 'r', encoding='utf-8') as f:
            csv_content = f.read()

        nodes = PowerShellClusterProvider.parse_machine_info_csv(csv_content)

        assert isinstance(nodes, list)
        assert len(nodes) > 0

        # Verify first node structure
        first_node = nodes[0]
        assert isinstance(first_node, Node)
        assert first_node.name != ""
        assert first_node.type != ""
        assert first_node.host != ""

    def test_parse_machine_info_csv_extracts_correct_fields(self):
        """Test that CSV parsing extracts correct fields."""
        csv_content = """#Version:1.0
#Fields:MachineName,PodName,MachineFunction,Port,Image,SKU,StaticIP,ScaleUnit,staticFunction,staticScaleunit,status,repair,netMask,Freeze,FreezeEndsAt,ConnectedTo,Enclosure,Environment,PhysicalMachineName
MWHEEEAP003CB01,POD_MWHE01,CH,0,Linux.retail.amd64,AzureSSVM,10.213.196.158,600,CH,600,H,None,0,0,1900-01-01,,,MTTitanMetricsBE-Prod-MWHE01,MWHEEEAP003CB01
#EOF"""

        nodes = PowerShellClusterProvider.parse_machine_info_csv(csv_content)

        assert len(nodes) == 1
        node = nodes[0]
        assert node.name == "MWHEEEAP003CB01"
        assert node.type == "CH"
        assert node.host == "10.213.196.158"
        assert node.attributes["status"] == "H"
        assert node.attributes["environment"] == "MTTitanMetricsBE-Prod-MWHE01"

    def test_parse_machine_info_csv_skips_comment_lines(self):
        """Test that comment lines are skipped."""
        csv_content = """#Version:1.0
#Fields:MachineName,PodName,MachineFunction,...
#This is a comment
MACHINE1,POD1,CH,0,Image,SKU,10.0.0.1,100,CH,100,H,None,0,0,1900-01-01,,,Cluster1,MACHINE1
#EOF"""

        nodes = PowerShellClusterProvider.parse_machine_info_csv(csv_content)

        assert len(nodes) == 1
        assert nodes[0].name == "MACHINE1"

    def test_parse_machine_info_csv_handles_empty_content(self):
        """Test handling of empty CSV content."""
        nodes = PowerShellClusterProvider.parse_machine_info_csv("")
        assert nodes == []

    def test_parse_machine_info_csv_handles_only_comments(self):
        """Test handling of CSV with only comments."""
        csv_content = """#Version:1.0
#Fields:MachineName,...
#EOF"""
        nodes = PowerShellClusterProvider.parse_machine_info_csv(csv_content)
        assert nodes == []

    def test_parse_machine_info_csv_multiple_nodes(self):
        """Test parsing multiple nodes from CSV, excluding UTILITY nodes."""
        csv_content = """#Version:1.0
#Fields:MachineName,PodName,MachineFunction,Port,Image,SKU,StaticIP,ScaleUnit,staticFunction,staticScaleunit,status,repair,netMask,Freeze,FreezeEndsAt,ConnectedTo,Enclosure,Environment,PhysicalMachineName
NODE001,POD1,CH,0,Image,SKU,10.0.0.1,100,CH,100,H,None,0,0,1900-01-01,,,Cluster1,NODE001
NODE002,POD1,CH,0,Image,SKU,10.0.0.2,200,CH,200,H,None,0,0,1900-01-01,,,Cluster1,NODE002
NODE003,POD1,UTILITY,0,Image,SKU,10.0.0.3,100,UTILITY,100,P,None,0,0,1900-01-01,,,Cluster1,NODE003
#EOF"""

        nodes = PowerShellClusterProvider.parse_machine_info_csv(csv_content)

        # UTILITY nodes are filtered out, so only 2 CH nodes should remain
        assert len(nodes) == 2
        assert nodes[0].name == "NODE001"
        assert nodes[1].name == "NODE002"
        # Verify UTILITY node is not included
        node_names = [n.name for n in nodes]
        assert "NODE003" not in node_names

    def test_parse_machine_info_csv_node_with_different_status(self):
        """Test nodes with different status values."""
        csv_content = """#Version:1.0
#Fields:MachineName,PodName,MachineFunction,Port,Image,SKU,StaticIP,ScaleUnit,staticFunction,staticScaleunit,status,repair,netMask,Freeze,FreezeEndsAt,ConnectedTo,Enclosure,Environment,PhysicalMachineName
HEALTHY_NODE,POD1,CH,0,Image,SKU,10.0.0.1,100,CH,100,H,None,0,0,1900-01-01,,,Cluster1,HEALTHY_NODE
PENDING_NODE,POD1,CH,0,Image,SKU,10.0.0.2,100,CH,100,P,None,0,0,1900-01-01,,,Cluster1,PENDING_NODE
#EOF"""

        nodes = PowerShellClusterProvider.parse_machine_info_csv(csv_content)

        assert len(nodes) == 2
        assert nodes[0].attributes["status"] == "H"
        assert nodes[1].attributes["status"] == "P"

    def test_parse_actual_machineinfo_file_node_count(self):
        """Test that actual machineinfo.csv has expected number of CH nodes (excluding UTILITY)."""
        with open(TEST_MACHINEINFO_CSV, 'r', encoding='utf-8') as f:
            csv_content = f.read()

        nodes = PowerShellClusterProvider.parse_machine_info_csv(csv_content)

        # The file has 40 total nodes, but 1 is UTILITY, so 39 CH nodes remain
        assert len(nodes) == 39
        # Verify no UTILITY nodes are included
        for node in nodes:
            assert node.type != "UTILITY"

    def test_parse_actual_machineinfo_file_specific_node(self):
        """Test that specific node MWHEEEAP003CB01 is correctly parsed."""
        with open(TEST_MACHINEINFO_CSV, 'r', encoding='utf-8') as f:
            csv_content = f.read()

        nodes = PowerShellClusterProvider.parse_machine_info_csv(csv_content)
        node_names = [n.name for n in nodes]

        # Check that MWHEEEAP003CB01 is in the list
        assert "MWHEEEAP003CB01" in node_names

        # Find and verify the specific node
        target_node = next(n for n in nodes if n.name == "MWHEEEAP003CB01")
        assert target_node.type == "CH"
        assert target_node.host == "10.213.196.158"
        assert target_node.attributes["status"] == "H"
        assert target_node.attributes["environment"] == "MTTitanMetricsBE-Prod-MWHE01"

    def test_extract_region_from_cluster_name(self):
        """Test that region is correctly extracted from cluster name."""
        # Test normal cluster names
        assert PowerShellClusterProvider._extract_region("MTTitanMetricsBE-Prod-MWHE01") == "MWHE01"
        assert PowerShellClusterProvider._extract_region("MyCluster-Dev-USWEST2") == "USWEST2"
        assert PowerShellClusterProvider._extract_region("Simple-Region") == "Region"

        # Test edge cases
        assert PowerShellClusterProvider._extract_region("SingleName") == "SingleName"
        assert PowerShellClusterProvider._extract_region("") == ""
        assert PowerShellClusterProvider._extract_region("A-B-C-D") == "D"
