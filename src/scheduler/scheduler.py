"""
Scheduler Module

This module provides a scheduling system for periodic metric collection
and alert evaluation.
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
from typing import List, Optional, Callable
import logging

from src.cluster import ClusterInfoProvider
from src.metrics import MetricRegistry, MetricStorage
from src.alerts import AlertManager

logger = logging.getLogger(__name__)


class CollectionScheduler:
    """Scheduler for periodic metric collection and alert evaluation."""

    def __init__(self,
                 cluster_provider: ClusterInfoProvider,
                 metric_registry: MetricRegistry,
                 metric_storage: MetricStorage,
                 alert_manager: AlertManager,
                 interval_seconds: int = 60):
        self.cluster_provider = cluster_provider
        self.metric_registry = metric_registry
        self.metric_storage = metric_storage
        self.alert_manager = alert_manager
        self.interval_seconds = interval_seconds

        self._scheduler = BackgroundScheduler()
        self._running = False
        self._callbacks: List[Callable] = []

    def add_collection_callback(self, callback: Callable) -> None:
        """Add a callback to be called after each collection cycle."""
        self._callbacks.append(callback)

    def start(self) -> None:
        """Start the collection scheduler."""
        if self._running:
            return

        self._scheduler.add_job(
            self._collection_cycle,
            trigger=IntervalTrigger(seconds=self.interval_seconds),
            id='metric_collection',
            name='Metric Collection Job',
            replace_existing=True
        )

        self._scheduler.start()
        self._running = True
        logger.info(f"Collection scheduler started with interval of {self.interval_seconds} seconds")

        # Run initial collection
        self._collection_cycle()

    def stop(self) -> None:
        """Stop the collection scheduler."""
        if not self._running:
            return

        self._scheduler.shutdown(wait=False)
        self._running = False
        logger.info("Collection scheduler stopped")

    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._running

    def trigger_collection(self) -> None:
        """Manually trigger a collection cycle."""
        self._collection_cycle()

    def _collection_cycle(self) -> None:
        """Execute a single collection cycle for all nodes."""
        logger.info("Starting metric collection cycle")

        clusters = self.cluster_provider.get_clusters()
        total_metrics = 0
        total_alerts = 0

        for cluster in clusters:
            for node in cluster.nodes:
                try:
                    # Collect metrics for this node
                    metrics = self.metric_registry.collect_all(
                        node_name=node.name,
                        cluster_name=cluster.name
                    )

                    # Store metrics
                    self.metric_storage.store_batch(metrics)
                    total_metrics += len(metrics)

                    # Evaluate alerts for each metric
                    for metric in metrics:
                        try:
                            value = float(metric.value)
                            alerts = self.alert_manager.evaluate_metric(
                                metric_name=metric.metric_name,
                                metric_value=value,
                                node_name=node.name,
                                cluster_name=cluster.name
                            )
                            total_alerts += len(alerts)
                        except (ValueError, TypeError):
                            # Skip non-numeric metrics for alert evaluation
                            pass

                except Exception as e:
                    logger.error(f"Failed to collect metrics for {cluster.name}/{node.name}: {e}")

        logger.info(f"Collection cycle completed: {total_metrics} metrics, {total_alerts} alerts")

        # Execute callbacks
        for callback in self._callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"Callback execution failed: {e}")
