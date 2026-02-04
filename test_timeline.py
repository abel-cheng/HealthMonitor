"""测试健康状态时间序列功能"""
from src.metrics.collector import MetricStorage
from datetime import datetime, timedelta

storage = MetricStorage()
base_time = datetime.utcnow()
start = base_time - timedelta(hours=2)
end = base_time

print("=== 健康状态时间序列 ===")
print("时间                     状态  说明")
print("-" * 50)

timeline = storage.get_health_timeline('test-cluster', '13.66.204.72', start, end)
for t in timeline:
    status_icon = "✓" if t['status'] == 1 else "✗"
    change_mark = f" *** {t['change_type']}" if t['changed'] else ""
    print(f"{t['timestamp'][:19]}   {status_icon} {t['status']}   {t['status_text']}{change_mark}")

print("\n=== 健康状态摘要 ===")
summary = storage.get_health_summary('test-cluster', '13.66.204.72', start, end)
print(f"节点: {summary['node_name']}")
print(f"集群: {summary['cluster_name']}")
print(f"检查次数: {summary['total_checks']}")
print(f"健康次数: {summary['healthy_count']}")
print(f"离线次数: {summary['unhealthy_count']}")
print(f"可用率: {summary['availability_percent']}%")
print(f"当前状态: {summary['current_status']}")

if summary['status_changes']:
    print(f"\n状态变化记录 ({len(summary['status_changes'])} 次):")
    for change in summary['status_changes']:
        print(f"  {change['timestamp'][:19]} -> {change['change_type']}")
else:
    print("\n无状态变化")
