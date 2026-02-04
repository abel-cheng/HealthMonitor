"""
Web Application Module for JSON Metric Logs Visualization

This module provides a Flask-based web interface for visualizing cluster health
based on JSON metric logs.

Features:
- Cluster list view with status indicators (green/yellow/red)
- Node list view for each cluster
- Time series chart for node status history
"""

from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import os
import sys

# Import JSON storage reader
from json_storage import JsonMetricReader


def create_app(base_dir: str) -> Flask:
    """
    Create and configure the Flask application.
    
    Args:
        base_dir: Base directory containing JSON metric logs
    
    Returns:
        Configured Flask application
    """
    app = Flask(__name__,
                template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
                static_folder=os.path.join(os.path.dirname(__file__), 'static'))
    
    # Initialize JSON metric reader
    reader = JsonMetricReader(base_dir)
    
    @app.route('/')
    def index():
        """Render the main dashboard page."""
        return render_template('dashboard.html')
    
    @app.route('/api/clusters')
    def get_clusters():
        """
        Get all clusters with their current status.
        
        Returns JSON:
        [
            {
                "name": "cluster_name",
                "total_nodes": 10,
                "healthy_nodes": 9,
                "down_nodes": 1,
                "status": "unstable",
                "status_color": "yellow",
                "last_check": "2026-02-04T11:45:00"
            }
        ]
        """
        statuses = reader.get_all_clusters_status()
        
        result = []
        for cluster in statuses:
            result.append({
                'name': cluster.name,
                'total_nodes': cluster.total_nodes,
                'healthy_nodes': cluster.healthy_nodes,
                'down_nodes': cluster.down_nodes,
                'status': cluster.status,
                'status_color': cluster.status_color,
                'last_check': cluster.last_check
            })
        
        return jsonify(result)
    
    @app.route('/api/clusters/<cluster_name>')
    def get_cluster(cluster_name: str):
        """
        Get detailed information about a specific cluster.
        
        Returns JSON:
        {
            "name": "cluster_name",
            "total_nodes": 10,
            "healthy_nodes": 9,
            "down_nodes": 1,
            "status": "unstable",
            "status_color": "yellow",
            "last_check": "...",
            "nodes": [
                {
                    "name": "node_name",
                    "status": 1,
                    "status_text": "healthy",
                    "status_color": "green",
                    "last_check": "..."
                }
            ]
        }
        """
        cluster = reader.get_cluster_status(cluster_name)
        if not cluster:
            return jsonify({'error': 'Cluster not found'}), 404
        
        nodes = []
        for node in cluster.nodes:
            nodes.append({
                'name': node.name,
                'status': node.status,
                'status_text': node.status_text,
                'status_color': node.status_color,
                'last_check': node.last_check
            })
        
        return jsonify({
            'name': cluster.name,
            'total_nodes': cluster.total_nodes,
            'healthy_nodes': cluster.healthy_nodes,
            'down_nodes': cluster.down_nodes,
            'status': cluster.status,
            'status_color': cluster.status_color,
            'last_check': cluster.last_check,
            'nodes': nodes
        })
    
    @app.route('/api/clusters/<cluster_name>/nodes/<node_name>')
    def get_node(cluster_name: str, node_name: str):
        """
        Get detailed information about a specific node.
        
        Returns JSON:
        {
            "name": "node_name",
            "cluster_name": "cluster_name",
            "status": 1,
            "status_text": "healthy",
            "status_color": "green",
            "last_check": "..."
        }
        """
        node = reader.get_node_status(cluster_name, node_name)
        if not node:
            return jsonify({'error': 'Node not found'}), 404
        
        return jsonify({
            'name': node.name,
            'cluster_name': node.cluster_name,
            'status': node.status,
            'status_text': node.status_text,
            'status_color': node.status_color,
            'last_check': node.last_check
        })
    
    @app.route('/api/clusters/<cluster_name>/nodes/<node_name>/history')
    def get_node_history(cluster_name: str, node_name: str):
        """
        Get time-series history for a node.
        
        Query params:
            hours: Number of hours to look back (default: 24)
        
        Returns JSON:
        {
            "cluster_name": "...",
            "node_name": "...",
            "hours": 24,
            "data": [
                {
                    "timestamp": "2026-02-04T10:00:00",
                    "value": 1,
                    "status_text": "healthy"
                }
            ]
        }
        """
        hours = int(request.args.get('hours', 24))
        history = reader.get_node_history(cluster_name, node_name, hours)
        
        return jsonify({
            'cluster_name': cluster_name,
            'node_name': node_name,
            'hours': hours,
            'data': history
        })
    
    return app


def run_server(base_dir: str, host: str = '0.0.0.0', port: int = 5000, debug: bool = True):
    """
    Run the web server.
    
    Args:
        base_dir: Base directory containing JSON metric logs
        host: Host address to bind
        port: Port number
        debug: Enable debug mode
    """
    app = create_app(base_dir)
    print(f"Starting Health Monitor Web Server...")
    print(f"Metrics directory: {base_dir}")
    print(f"Server running at http://{host}:{port}")
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Health Monitor Web Server')
    parser.add_argument('--metrics-dir', '-m', 
                        default=r'D:\ServiceHealthMatrixLogs',
                        help='Directory containing JSON metric logs')
    parser.add_argument('--host', default='0.0.0.0',
                        help='Host address to bind (default: 0.0.0.0)')
    parser.add_argument('--port', '-p', type=int, default=5000,
                        help='Port number (default: 5000)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode')
    
    args = parser.parse_args()
    run_server(args.metrics_dir, args.host, args.port, args.debug)
