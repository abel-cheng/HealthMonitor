#!/usr/bin/env python3
"""
ClickHouse Ping Collector

定期采集集群中各节点的 ClickHouse 健康状态（ping），增量输出结果。
适合系统定时任务（如 cron/Task Scheduler）周期性执行。

Usage:
    python collect_ping.py --cluster <cluster_name> --provider <provider_type>
    python collect_ping.py --cluster test-cluster --provider file
    python collect_ping.py --cluster MTTitanMetricsBE-Prod-MWHE01 --provider powershell --region MWHE01

Examples:
    # 使用文件配置
    python collect_ping.py --cluster test-cluster --provider file

    # 使用 PowerShell/dmclient 获取集群信息
    python collect_ping.py --cluster MTTitanMetricsBE-Prod-MWHE01 --provider powershell --region MWHE01

    # 单节点测试（保存到日志文件）
    python collect_ping.py --host 13.66.204.72 --cluster test-cluster --save
"""

import argparse
import os
import sys
from datetime import datetime
from typing import List, Optional

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.metrics.collector import ClickHouseStatusCollector, MetricValue
from src.cluster.provider import (
    ClusterProviderFactory, 
    FileClusterProvider, 
    PowerShellClusterProvider,
    Node
)


def collect_node_status(node: Node, cluster_name: str, 
                        port: int = 8123, timeout: int = 10) -> MetricValue:
    """采集单个节点的 ping 状态"""
    collector = ClickHouseStatusCollector(
        host=node.host,
        port=port,
        timeout=timeout
    )
    return collector.collect(node_name=node.name, cluster_name=cluster_name)


# Metric ID 定义
METRIC_ID_PING = "ch_ping"  # ClickHouse ping 状态


def format_log_line(metric: MetricValue, metric_id: str) -> str:
    """格式化为日志行：metric_id,timestamp,status"""
    return f"{metric_id},{metric.timestamp[:19]},{metric.value}"


def print_result(metric: MetricValue, verbose: bool = False):
    """增量式输出采集结果"""
    status_icon = "✓" if metric.value == 1 else "✗"
    status_text = "OK" if metric.value == 1 else "FAIL"
    timestamp = metric.timestamp[:19]
    
    # 简洁输出格式，适合日志追加
    print(f"[{timestamp}] {metric.cluster_name}/{metric.node_name}: {status_icon} {status_text}")


def save_to_log(metrics: List[MetricValue], data_dir: str, cluster_name: str, metric_id: str):
    """
    保存到日志文件，按日期和 machine 组织目录，增量追加。
    格式: metric_id,timestamp,status
    文件: data/metrics/<cluster>/<date>/<node>.log
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    date_dir = os.path.join(data_dir, cluster_name, today)
    os.makedirs(date_dir, exist_ok=True)
    
    for metric in metrics:
        # 每个节点一个 log 文件
        log_file = os.path.join(date_dir, f"{metric.node_name}.log")
        
        # 如果是新文件，写入表头
        write_header = not os.path.exists(log_file)
        
        with open(log_file, 'a', encoding='utf-8') as f:
            if write_header:
                f.write("# metric_id,timestamp,status\n")
            f.write(format_log_line(metric, metric_id) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="ClickHouse Ping Collector - 采集集群节点健康状态",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 集群参数
    parser.add_argument("--cluster", "-c", required=True, help="集群名称")
    parser.add_argument("--provider", "-p", default="file", 
                        choices=["file", "powershell"],
                        help="Provider类型 (default: file)")
    
    # Provider 配置
    parser.add_argument("--config", default="config/clusters.yaml",
                        help="集群配置文件路径 (file provider)")
    parser.add_argument("--region", help="区域代码 (powershell provider)")
    parser.add_argument("--dmclient", default=".\\dmclient.exe",
                        help="dmclient.exe 路径 (powershell provider)")
    parser.add_argument("--function", default="CH",
                        help="Machine function (powershell provider)")
    
    # 单节点模式
    parser.add_argument("--host", help="直接指定单个节点 host")
    
    # ClickHouse 连接参数
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP 端口")
    parser.add_argument("--timeout", type=int, default=10, help="请求超时秒数")
    
    # 存储参数
    parser.add_argument("--data-dir", default="data/metrics", help="数据存储目录")
    parser.add_argument("--save", "-s", action="store_true", help="保存到日志文件（按天增量追加）")
    
    # 输出控制
    parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    parser.add_argument("--quiet", "-q", action="store_true", help="静默模式，仅输出失败节点")
    
    args = parser.parse_args()
    
    # 获取节点列表
    nodes: List[Node] = []
    
    if args.host:
        # 单节点模式
        nodes = [Node(
            name=args.host,
            type="CH",
            host=args.host,
            collection_method="remote"
        )]
    else:
        # 从 Provider 获取节点
        try:
            if args.provider == "file":
                provider = FileClusterProvider(args.config)
            elif args.provider == "powershell":
                if not args.region:
                    print("Error: --region is required for powershell provider")
                    sys.exit(1)
                provider = PowerShellClusterProvider(
                    region=args.region,
                    cluster_name=args.cluster,
                    dmclient_path=args.dmclient,
                    machine_function=args.function
                )
            
            cluster = provider.get_cluster(args.cluster)
            if cluster:
                nodes = cluster.nodes
            else:
                print(f"Error: Cluster '{args.cluster}' not found")
                sys.exit(1)
                
        except Exception as e:
            print(f"Error loading cluster info: {e}")
            sys.exit(1)
    
    if not nodes:
        print(f"Warning: No nodes found for cluster '{args.cluster}'")
        sys.exit(0)
    
    # 采集所有节点
    metrics: List[MetricValue] = []
    failed_count = 0
    
    for node in nodes:
        metric = collect_node_status(
            node=node,
            cluster_name=args.cluster,
            port=args.port,
            timeout=args.timeout
        )
        metrics.append(metric)
        
        # 输出结果
        if metric.value == 0:
            failed_count += 1
            print_result(metric, args.verbose)
        elif not args.quiet:
            print_result(metric, args.verbose)
    
    # 保存到日志文件（增量追加，每个节点一个文件）
    if args.save:
        save_to_log(metrics, args.data_dir, args.cluster, METRIC_ID_PING)
        if not args.quiet:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            log_dir = os.path.join(args.data_dir, args.cluster, today)
            print(f"--- Saved to: {log_dir}/<node>.log ---")
    
    # 汇总
    if not args.quiet:
        total = len(nodes)
        ok = total - failed_count
        print(f"--- {args.cluster}: {ok}/{total} nodes OK ---")
    
    # 返回码：有失败节点时返回 1
    sys.exit(1 if failed_count > 0 else 0)


if __name__ == "__main__":
    main()
