"""
Standalone script to export cluster info as JSON using PowerShellClusterProvider.
"""
import sys
import json
import os
from src.cluster.provider import PowerShellClusterProvider

def main():
    # Default config, can be replaced by CLI args or config file
    cluster_name = os.environ.get('CLUSTER_NAME', 'MTTitanMetricsBE-Prod-MWHE01')
    dmclient_path = os.environ.get('DMCLIENT_PATH', '.\\dmclient.exe')
    machine_function = os.environ.get('MACHINE_FUNCTION', 'CH')

    # Optionally support CLI args: cluster_name [dmclient_path] [machine_function]
    if len(sys.argv) > 1:
        cluster_name = sys.argv[1]
        if len(sys.argv) > 2:
            dmclient_path = sys.argv[2]
        if len(sys.argv) > 3:
            machine_function = sys.argv[3]

    provider = PowerShellClusterProvider(
        cluster_name=cluster_name,
        dmclient_path=dmclient_path,
        machine_function=machine_function
    )
    clusters = provider.get_clusters()
    # Convert dataclasses to dicts for JSON serialization
    def cluster_to_dict(cluster):
        return {
            'name': cluster.name,
            'description': cluster.description,
            'nodes': [
                {
                    'name': node.name,
                    'type': node.type,
                    'host': node.host,
                    'collection_method': node.collection_method,
                    'attributes': node.attributes
                } for node in cluster.nodes
            ]
        }
    clusters_json = [cluster_to_dict(c) for c in clusters]
    print(json.dumps(clusters_json, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
