"""
Alert System Module

This module provides an extensible alerting framework that evaluates metrics
against configurable rules and triggers corresponding actions.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
import yaml
import json
import os
import logging
import requests


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AlertEvent:
    """Represents an alert event."""
    alert_id: str
    rule_name: str
    metric_name: str
    node_name: str
    cluster_name: str
    current_value: Any
    threshold: Any
    operator: str
    severity: str
    timestamp: str
    message: str


class AlertAction(ABC):
    """Abstract base class for alert actions."""

    @abstractmethod
    def execute(self, alert: AlertEvent) -> bool:
        """Execute the alert action. Returns True if successful."""
        pass


class LogAlertAction(AlertAction):
    """Action that logs alerts to the application log."""

    def __init__(self, level: str = "warning"):
        self.level = level.lower()

    def execute(self, alert: AlertEvent) -> bool:
        log_func = getattr(logger, self.level, logger.warning)
        message = (
            f"ALERT [{alert.severity.upper()}] {alert.rule_name}: "
            f"{alert.metric_name} = {alert.current_value} {alert.operator} {alert.threshold} "
            f"on {alert.cluster_name}/{alert.node_name}"
        )
        log_func(message)
        return True


class WebhookAlertAction(AlertAction):
    """Action that sends alerts to a webhook URL."""

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None):
        self.url = url
        self.headers = headers or {"Content-Type": "application/json"}

    def execute(self, alert: AlertEvent) -> bool:
        try:
            payload = {
                "alert_id": alert.alert_id,
                "rule_name": alert.rule_name,
                "metric_name": alert.metric_name,
                "node_name": alert.node_name,
                "cluster_name": alert.cluster_name,
                "current_value": alert.current_value,
                "threshold": alert.threshold,
                "severity": alert.severity,
                "timestamp": alert.timestamp,
                "message": alert.message
            }
            response = requests.post(
                self.url,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            return response.status_code < 400
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")
            return False


class EmailAlertAction(AlertAction):
    """Action that sends alerts via email (placeholder implementation)."""

    def __init__(self, to: str, smtp_server: str = "", smtp_port: int = 587):
        self.to = to
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def execute(self, alert: AlertEvent) -> bool:
        # Placeholder - actual implementation would use smtplib
        logger.info(f"Would send email to {self.to}: {alert.message}")
        return True


class FileAlertAction(AlertAction):
    """Action that writes alerts to a file."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

    def execute(self, alert: AlertEvent) -> bool:
        try:
            with open(self.file_path, 'a', encoding='utf-8') as f:
                alert_data = {
                    "alert_id": alert.alert_id,
                    "rule_name": alert.rule_name,
                    "metric_name": alert.metric_name,
                    "node_name": alert.node_name,
                    "cluster_name": alert.cluster_name,
                    "current_value": alert.current_value,
                    "threshold": alert.threshold,
                    "severity": alert.severity,
                    "timestamp": alert.timestamp,
                    "message": alert.message
                }
                f.write(json.dumps(alert_data) + "\n")
            return True
        except Exception as e:
            logger.error(f"Failed to write alert to file: {e}")
            return False


class CustomAlertAction(AlertAction):
    """Action that executes a custom callback function."""

    def __init__(self, callback: Callable[[AlertEvent], bool]):
        self.callback = callback

    def execute(self, alert: AlertEvent) -> bool:
        try:
            return self.callback(alert)
        except Exception as e:
            logger.error(f"Custom alert action failed: {e}")
            return False


@dataclass
class AlertRule:
    """Represents an alert rule configuration."""
    name: str
    metric: str
    operator: str  # >, <, >=, <=, ==, !=
    threshold: float
    severity: str  # info, warning, critical
    actions: List[AlertAction] = field(default_factory=list)
    enabled: bool = True
    cooldown_seconds: int = 300  # Minimum time between alerts

    _last_triggered: Dict[str, datetime] = field(default_factory=dict, repr=False)

    def evaluate(self, metric_value: float) -> bool:
        """Evaluate if the metric value violates the rule."""
        operators = {
            '>': lambda x, y: x > y,
            '<': lambda x, y: x < y,
            '>=': lambda x, y: x >= y,
            '<=': lambda x, y: x <= y,
            '==': lambda x, y: x == y,
            '!=': lambda x, y: x != y,
        }
        op_func = operators.get(self.operator)
        if op_func is None:
            return False
        return op_func(metric_value, self.threshold)

    def should_trigger(self, node_key: str) -> bool:
        """Check if the rule should trigger (considering cooldown)."""
        if not self.enabled:
            return False

        last = self._last_triggered.get(node_key)
        if last is None:
            return True

        elapsed = (datetime.utcnow() - last).total_seconds()
        return elapsed >= self.cooldown_seconds

    def mark_triggered(self, node_key: str) -> None:
        """Mark the rule as triggered for a node."""
        self._last_triggered[node_key] = datetime.utcnow()


class AlertActionFactory:
    """Factory for creating alert actions."""

    @staticmethod
    def create(action_type: str, params: Dict[str, Any]) -> AlertAction:
        """Create an alert action based on type and parameters."""
        if action_type == "log":
            return LogAlertAction(level=params.get("level", "warning"))
        elif action_type == "webhook":
            return WebhookAlertAction(
                url=params.get("url", ""),
                headers=params.get("headers")
            )
        elif action_type == "email":
            return EmailAlertAction(
                to=params.get("to", ""),
                smtp_server=params.get("smtp_server", ""),
                smtp_port=params.get("smtp_port", 587)
            )
        elif action_type == "file":
            return FileAlertAction(file_path=params.get("file_path", "alerts.log"))
        else:
            raise ValueError(f"Unknown action type: {action_type}")


class AlertManager:
    """Manages alert rules and evaluates metrics against them."""

    def __init__(self):
        self._rules: Dict[str, AlertRule] = {}
        self._alert_history: List[AlertEvent] = []

    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self._rules[rule.name] = rule

    def remove_rule(self, rule_name: str) -> None:
        """Remove an alert rule."""
        if rule_name in self._rules:
            del self._rules[rule_name]

    def get_rule(self, rule_name: str) -> Optional[AlertRule]:
        """Get a rule by name."""
        return self._rules.get(rule_name)

    def get_all_rules(self) -> List[AlertRule]:
        """Get all rules."""
        return list(self._rules.values())

    def evaluate_metric(self, metric_name: str, metric_value: float,
                        node_name: str, cluster_name: str) -> List[AlertEvent]:
        """Evaluate a metric against all applicable rules."""
        alerts = []
        node_key = f"{cluster_name}/{node_name}"

        for rule in self._rules.values():
            if rule.metric != metric_name:
                continue

            if not rule.evaluate(metric_value):
                continue

            if not rule.should_trigger(node_key):
                continue

            # Create alert event
            alert = AlertEvent(
                alert_id=f"{rule.name}-{node_key}-{datetime.utcnow().timestamp()}",
                rule_name=rule.name,
                metric_name=metric_name,
                node_name=node_name,
                cluster_name=cluster_name,
                current_value=metric_value,
                threshold=rule.threshold,
                operator=rule.operator,
                severity=rule.severity,
                timestamp=datetime.utcnow().isoformat(),
                message=f"Alert: {metric_name} = {metric_value} {rule.operator} {rule.threshold}"
            )

            # Execute actions
            for action in rule.actions:
                try:
                    action.execute(alert)
                except Exception as e:
                    logger.error(f"Failed to execute alert action: {e}")

            rule.mark_triggered(node_key)
            self._alert_history.append(alert)
            alerts.append(alert)

        return alerts

    def get_alert_history(self, limit: int = 100) -> List[AlertEvent]:
        """Get recent alert history."""
        return self._alert_history[-limit:]

    def clear_alert_history(self) -> None:
        """Clear alert history."""
        self._alert_history.clear()

    def load_rules_from_file(self, file_path: str) -> None:
        """Load alert rules from a YAML configuration file."""
        if not os.path.exists(file_path):
            return

        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        if not data or 'rules' not in data:
            return

        for rule_data in data['rules']:
            actions = []
            for action_data in rule_data.get('actions', []):
                try:
                    action = AlertActionFactory.create(
                        action_data['type'],
                        action_data.get('params', {})
                    )
                    actions.append(action)
                except Exception as e:
                    logger.warning(f"Failed to create action: {e}")

            rule = AlertRule(
                name=rule_data['name'],
                metric=rule_data['metric'],
                operator=rule_data['operator'],
                threshold=rule_data['threshold'],
                severity=rule_data.get('severity', 'warning'),
                actions=actions,
                enabled=rule_data.get('enabled', True),
                cooldown_seconds=rule_data.get('cooldown_seconds', 300)
            )
            self.add_rule(rule)
