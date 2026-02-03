"""
Web Application Module

This module provides a Flask-based web interface for visualizing cluster health,
node metrics, and historical time-series data.
"""

from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cluster import ClusterProviderFactory, Cluster
from metrics import MetricStorage, MetricRegistry


def create_app(cluster_provider, metric_storage: MetricStorage,
               metric_registry: MetricRegistry) -> Flask:
    """Create and configure the Flask application."""

    app = Flask(__name__,
                template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
                static_folder=os.path.join(os.path.dirname(__file__), 'static'))

    @app.route('/')
    def index():
        """Render the main dashboard page."""
        return render_template('index.html')

    @app.route('/api/clusters')
    def get_clusters():
        """Get all clusters with their current status."""
        clusters = cluster_provider.get_clusters()
        result = []

        for cluster in clusters:
            cluster_status = calculate_cluster_status(cluster, metric_storage)
            result.append({
                'name': cluster.name,
                'description': cluster.description,
                'node_count': len(cluster.nodes),
                'status': cluster_status['status'],
                'status_color': cluster_status['color'],
                'healthy_nodes': cluster_status['healthy_nodes'],
                'warning_nodes': cluster_status['warning_nodes'],
                'critical_nodes': cluster_status['critical_nodes']
            })

        return jsonify(result)

    @app.route('/api/clusters/<cluster_name>')
    def get_cluster(cluster_name: str):
        """Get detailed information about a specific cluster."""
        cluster = cluster_provider.get_cluster(cluster_name)
        if not cluster:
            return jsonify({'error': 'Cluster not found'}), 404

        nodes = []
        for node in cluster.nodes:
            node_status = get_node_status(cluster_name, node.name, metric_storage)
            nodes.append({
                'name': node.name,
                'type': node.type,
                'host': node.host,
                'collection_method': node.collection_method,
                'status': node_status['status'],
                'status_color': node_status['color'],
                'metrics': node_status['metrics']
            })

        return jsonify({
            'name': cluster.name,
            'description': cluster.description,
            'nodes': nodes
        })

    @app.route('/api/clusters/<cluster_name>/nodes/<node_name>')
    def get_node(cluster_name: str, node_name: str):
        """Get detailed information about a specific node."""
        cluster = cluster_provider.get_cluster(cluster_name)
        if not cluster:
            return jsonify({'error': 'Cluster not found'}), 404

        node = cluster.get_node(node_name)
        if not node:
            return jsonify({'error': 'Node not found'}), 404

        node_status = get_node_status(cluster_name, node_name, metric_storage)

        return jsonify({
            'name': node.name,
            'type': node.type,
            'host': node.host,
            'collection_method': node.collection_method,
            'cluster_name': cluster_name,
            'status': node_status['status'],
            'status_color': node_status['color'],
            'metrics': node_status['metrics']
        })

    @app.route('/api/clusters/<cluster_name>/nodes/<node_name>/metrics')
    def get_node_metrics(cluster_name: str, node_name: str):
        """Get time-series metrics for a node."""
        metric_name = request.args.get('metric', 'cpu_percent')
        hours = int(request.args.get('hours', 24))

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        metrics = metric_storage.query(
            cluster_name, node_name,
            start_time, end_time,
            metric_name
        )

        # Format for chart display
        data_points = []
        for m in metrics:
            try:
                value = float(m['value']) if isinstance(m['value'], (int, float, str)) else 0
                data_points.append({
                    'timestamp': m['timestamp'],
                    'value': value
                })
            except (ValueError, TypeError):
                pass

        return jsonify({
            'metric_name': metric_name,
            'node_name': node_name,
            'cluster_name': cluster_name,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'data': data_points
        })

    @app.route('/api/metrics/available')
    def get_available_metrics():
        """Get list of available metrics."""
        collectors = metric_registry.get_all_collectors()
        metrics = []
        for collector in collectors:
            metrics.append({
                'name': collector.name,
                'unit': collector.unit,
                'interval': collector.interval
            })
        return jsonify(metrics)

    return app


def calculate_cluster_status(cluster: Cluster, storage: MetricStorage) -> Dict[str, Any]:
    """Calculate the overall status of a cluster based on node metrics."""
    healthy = 0
    warning = 0
    critical = 0

    for node in cluster.nodes:
        node_status = get_node_status(cluster.name, node.name, storage)
        if node_status['status'] == 'healthy':
            healthy += 1
        elif node_status['status'] == 'warning':
            warning += 1
        else:
            critical += 1

    total = len(cluster.nodes)
    if total == 0:
        return {
            'status': 'unknown',
            'color': 'gray',
            'healthy_nodes': 0,
            'warning_nodes': 0,
            'critical_nodes': 0
        }

    # Determine overall status
    if critical > 0:
        status = 'critical'
        color = 'red'
    elif warning > 0:
        status = 'warning'
        color = 'yellow'
    else:
        status = 'healthy'
        color = 'green'

    return {
        'status': status,
        'color': color,
        'healthy_nodes': healthy,
        'warning_nodes': warning,
        'critical_nodes': critical
    }


def get_node_status(cluster_name: str, node_name: str,
                    storage: MetricStorage) -> Dict[str, Any]:
    """Get the current status and latest metrics for a node."""
    latest_metrics = storage.get_latest(cluster_name, node_name)

    # Group by metric name, keep latest value
    metrics_dict = {}
    for m in latest_metrics:
        metric_name = m['metric_name']
        if metric_name not in metrics_dict or \
           m['timestamp'] > metrics_dict[metric_name]['timestamp']:
            metrics_dict[metric_name] = m

    # Determine node status based on metrics
    status = 'healthy'
    color = 'green'

    # Check node_status metric
    node_status_metric = metrics_dict.get('node_status')
    if node_status_metric and node_status_metric['value'] == 0:
        status = 'critical'
        color = 'red'
    else:
        # Check CPU
        cpu_metric = metrics_dict.get('cpu_percent')
        if cpu_metric:
            cpu_value = float(cpu_metric['value'])
            if cpu_value > 90:
                status = 'critical'
                color = 'red'
            elif cpu_value > 70:
                status = 'warning'
                color = 'yellow'

        # Check memory
        mem_metric = metrics_dict.get('memory_percent')
        if mem_metric and status != 'critical':
            mem_value = float(mem_metric['value'])
            if mem_value > 90:
                status = 'critical'
                color = 'red'
            elif mem_value > 80 and status != 'warning':
                status = 'warning'
                color = 'yellow'

        # Check disk
        disk_metric = metrics_dict.get('disk_percent')
        if disk_metric and status != 'critical':
            disk_value = float(disk_metric['value'])
            if disk_value > 95:
                status = 'critical'
                color = 'red'
            elif disk_value > 85 and status != 'warning':
                status = 'warning'
                color = 'yellow'

    # Check if metrics are stale (no data in last 5 minutes)
    if not latest_metrics:
        status = 'unknown'
        color = 'gray'

    # Format metrics for display
    formatted_metrics = []
    for name, m in metrics_dict.items():
        formatted_metrics.append({
            'name': name,
            'value': m['value'],
            'unit': m.get('unit', ''),
            'timestamp': m['timestamp']
        })

    return {
        'status': status,
        'color': color,
        'metrics': formatted_metrics
    }
