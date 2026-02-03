"""
Health Monitor Application Entry Point

This is the main entry point for the Cluster Health Monitoring application.
It initializes all components and starts the web server with metric collection.
"""

import os
import sys
import yaml
import logging
import argparse
from datetime import datetime

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.cluster import ClusterProviderFactory
from src.metrics import MetricStorage, create_default_registry
from src.alerts import AlertManager
from src.scheduler import CollectionScheduler
from src.web import create_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_settings(config_path: str = 'config/settings.yaml') -> dict:
    """Load application settings from YAML file."""
    default_settings = {
        'app': {'name': 'HealthMonitor', 'debug': True},
        'storage': {'metrics_dir': 'data/metrics', 'retention_days': 7},
        'collection': {'interval_seconds': 60, 'timeout_seconds': 30},
        'web': {'host': '0.0.0.0', 'port': 5000},
        'cluster_provider': {
            'type': 'file',
            'config': {'file_path': 'config/clusters.yaml'}
        }
    }

    if not os.path.exists(config_path):
        logger.warning(f"Settings file not found: {config_path}, using defaults")
        return default_settings

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            settings = yaml.safe_load(f)
            if settings:
                # Merge with defaults
                for key, value in default_settings.items():
                    if key not in settings:
                        settings[key] = value
                return settings
    except Exception as e:
        logger.error(f"Failed to load settings: {e}")

    return default_settings


def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(description='Cluster Health Monitor')
    parser.add_argument('--config', '-c', default='config/settings.yaml',
                        help='Path to settings file')
    parser.add_argument('--host', default=None,
                        help='Web server host')
    parser.add_argument('--port', '-p', type=int, default=None,
                        help='Web server port')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode')
    parser.add_argument('--no-collection', action='store_true',
                        help='Disable automatic metric collection')
    args = parser.parse_args()

    # Load settings
    settings = load_settings(args.config)

    logger.info(f"Starting {settings['app']['name']}")
    logger.info(f"Configuration loaded from: {args.config}")

    # Initialize cluster provider
    provider_config = settings['cluster_provider']
    cluster_provider = ClusterProviderFactory.create(
        provider_type=provider_config['type'],
        config=provider_config.get('config', {})
    )
    logger.info(f"Cluster provider initialized: {provider_config['type']}")

    # Log discovered clusters
    clusters = cluster_provider.get_clusters()
    logger.info(f"Discovered {len(clusters)} clusters")
    for cluster in clusters:
        logger.info(f"  - {cluster.name}: {len(cluster.nodes)} nodes")

    # Initialize metric storage
    metrics_dir = settings['storage']['metrics_dir']
    metric_storage = MetricStorage(base_dir=metrics_dir)
    logger.info(f"Metric storage initialized: {metrics_dir}")

    # Initialize metric registry with default collectors
    metric_registry = create_default_registry()
    collectors = metric_registry.get_all_collectors()
    logger.info(f"Registered {len(collectors)} metric collectors")

    # Initialize alert manager
    alert_manager = AlertManager()
    alerts_config = 'config/alerts.yaml'
    if os.path.exists(alerts_config):
        alert_manager.load_rules_from_file(alerts_config)
        logger.info(f"Loaded {len(alert_manager.get_all_rules())} alert rules")

    # Initialize scheduler
    scheduler = None
    if not args.no_collection:
        interval = settings['collection']['interval_seconds']
        scheduler = CollectionScheduler(
            cluster_provider=cluster_provider,
            metric_registry=metric_registry,
            metric_storage=metric_storage,
            alert_manager=alert_manager,
            interval_seconds=interval
        )
        scheduler.start()
        logger.info(f"Collection scheduler started (interval: {interval}s)")

    # Create and configure Flask app
    app = create_app(
        cluster_provider=cluster_provider,
        metric_storage=metric_storage,
        metric_registry=metric_registry
    )

    # Determine host and port
    host = args.host or settings['web']['host']
    port = args.port or settings['web']['port']
    debug = args.debug or settings['app']['debug']

    logger.info(f"Starting web server on http://{host}:{port}")

    try:
        app.run(host=host, port=port, debug=debug, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        if scheduler:
            scheduler.stop()
        logger.info("Application stopped")


if __name__ == '__main__':
    main()
