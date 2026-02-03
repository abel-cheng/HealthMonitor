import pytest
from src.alerts.manager import AlertManager, AlertRule, LogAlertAction

def test_alert_manager_add_and_evaluate_rule():
    manager = AlertManager()
    rule = AlertRule(
        name="test_high_cpu",
        metric="cpu_percent",
        operator=">",
        threshold=80,
        severity="critical",
        actions=[LogAlertAction(level="error")],
        enabled=True,
        cooldown_seconds=0
    )
    manager.add_rule(rule)
    alerts = manager.evaluate_metric(
        metric_name="cpu_percent",
        metric_value=90,
        node_name="test-node",
        cluster_name="test-cluster"
    )
    assert isinstance(alerts, list)
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.metric_name == "cpu_percent"
    assert alert.current_value == 90
    assert alert.severity == "critical"
