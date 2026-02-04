# Cluster Health Monitor

A comprehensive cluster health monitoring application built with Python. This application provides cluster information collection, metric gathering, web-based visualization, and alerting capabilities.

## Features

### 1. Cluster Information Collection
- **Extensible Provider Interface**: Support for multiple data sources
  - File-based configuration (YAML)
  - Database provider (extensible)
  - PowerShell command integration (Windows)
- **Hierarchical Structure**: Clusters contain nodes with configurable attributes

### 2. Metrics Collection
- **Built-in Collectors**:
  - CPU usage percentage
  - Memory usage (percentage and bytes)
  - Disk usage (percentage and bytes)
  - Network I/O (bytes sent/received)
  - Process count
  - System load average
  - Node availability status
- **Extensible Framework**: Easy to add custom metric collectors
- **Periodic Collection**: Configurable collection intervals
- **JSON Storage**: Metrics stored in hourly JSON files organized by cluster/node/date

### 3. Web Dashboard
- **Cluster Overview**: Visual matrix showing all clusters with status indicators
  - Green: All nodes healthy
  - Yellow: Some nodes have warnings
  - Red: Critical issues detected
- **Node List**: Detailed view of nodes within each cluster
- **Time Series Charts**: Historical metric visualization using Chart.js
- **Real-time Updates**: Refresh data on demand

### 4. Alerting System
- **Configurable Rules**: YAML-based alert rule configuration
- **Multiple Actions**:
  - Log alerts to application log
  - Send webhooks to external systems
  - Email notifications (configurable)
  - Write to alert files
- **Cooldown Support**: Prevent alert flooding
- **Severity Levels**: Info, Warning, Critical

## Project Structure

```
HealthMonitor/
├── config/
│   ├── settings.yaml      # Application settings
│   ├── clusters.yaml      # Cluster definitions
│   └── alerts.yaml        # Alert rules
├── data/
│   └── metrics/           # Stored metrics (auto-created)
├── src/
│   ├── cluster/           # Cluster info providers
│   │   ├── __init__.py
│   │   └── provider.py
│   ├── metrics/           # Metric collectors
│   │   ├── __init__.py
│   │   └── collector.py
│   ├── alerts/            # Alert management
│   │   ├── __init__.py
│   │   └── manager.py
│   ├── scheduler/         # Collection scheduler
│   │   ├── __init__.py
│   │   └── scheduler.py
│   ├── web/               # Web interface
│   │   ├── __init__.py
│   │   ├── app.py
│   │   └── templates/
│   │       └── index.html
│   └── __init__.py
├── main.py                # Application entry point
├── requirements.txt       # Python dependencies
└── README.md
```

## Installation

1. Clone or download this project

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   source venv/bin/activate  # Linux/macOS
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

### Application Settings (`config/settings.yaml`)
```yaml
app:
  name: "HealthMonitor"
  debug: true

storage:
  metrics_dir: "data/metrics"
  retention_days: 7

collection:
  interval_seconds: 60

web:
  host: "0.0.0.0"
  port: 5000

cluster_provider:
  type: "file"  # Options: file, database, powershell
  config:
    file_path: "config/clusters.yaml"
```

### Cluster Configuration (`config/clusters.yaml`)
```yaml
clusters:
  - name: "production-cluster"
    description: "Production environment"
    nodes:
      - name: "prod-node-01"
        type: "master"
        host: "192.168.1.10"
        collection_method: "ssh"
```

### Alert Rules (`config/alerts.yaml`)
```yaml
rules:
  - name: "high_cpu_usage"
    metric: "cpu_percent"
    operator: ">"
    threshold: 90
    severity: "critical"
    actions:
      - type: "log"
        params:
          level: "error"
      - type: "webhook"
        params:
          url: "http://your-webhook-url"
```

## Usage

### Start the Application
```bash
python main.py
```

### Command Line Options
```bash
python main.py --help

Options:
  --config, -c    Path to settings file (default: config/settings.yaml)
  --host          Web server host (default: from settings)
  --port, -p      Web server port (default: from settings)
  --debug         Enable debug mode
  --no-collection Disable automatic metric collection
```

### Access the Dashboard
Open a web browser and navigate to:
```
http://localhost:5000
```

## Extending the Application

### Adding Custom Metric Collectors

```python
from src.metrics import MetricCollector, MetricValue

class CustomMetricCollector(MetricCollector):
    def __init__(self, interval: int = 60):
        super().__init__(name="custom_metric", unit="", interval=interval)

    def collect(self, node_name: str, cluster_name: str, **kwargs) -> MetricValue:
        # Implement your metric collection logic
        value = your_collection_logic()
        return self._create_metric(node_name, cluster_name, value)

# Register the collector
metric_registry.register(CustomMetricCollector())
```

### Adding Custom Alert Actions

```python
from src.alerts import AlertAction, AlertEvent

class SlackAlertAction(AlertAction):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def execute(self, alert: AlertEvent) -> bool:
        # Implement Slack notification logic
        return True
```

### Adding Custom Cluster Providers

```python
from src.cluster import ClusterInfoProvider, Cluster

class CustomClusterProvider(ClusterInfoProvider):
    def get_clusters(self) -> List[Cluster]:
        # Implement your cluster discovery logic
        pass

    def get_cluster(self, cluster_name: str) -> Optional[Cluster]:
        # Implement single cluster retrieval
        pass

    def refresh(self) -> None:
        # Implement refresh logic
        pass
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard page |
| `/api/clusters` | GET | List all clusters with status |
| `/api/clusters/<name>` | GET | Get cluster details with nodes |
| `/api/clusters/<cluster>/nodes/<node>` | GET | Get node details |
| `/api/clusters/<cluster>/nodes/<node>/metrics` | GET | Get node metrics history |
| `/api/metrics/available` | GET | List available metrics |

### Query Parameters for Metrics API
- `metric`: Metric name (default: `cpu_percent`)
- `hours`: Time range in hours (default: `24`)

## Data Storage Format

Metrics are stored in JSON files organized by:
```
data/metrics/{cluster}/{node}/{year}/{month}/{day}/{hour}.json
```

Each file contains an array of metric records:
```json
[
  {
    "metric_id": "uuid",
    "metric_name": "cpu_percent",
    "value": 45.2,
    "timestamp": "2026-02-03T10:30:00",
    "node_name": "prod-node-01",
    "cluster_name": "production-cluster",
    "unit": "%",
    "tags": {}
  }
]
```

## Standalone Metric Collector (collector_cli)

The project includes a standalone metric collector (`collector_cli.py`) that can be compiled into an executable and run independently via cron jobs or Windows Task Scheduler.

### Features

- Supports multiple cluster info providers (PowerShell with dmclient.exe, file-based YAML)
- Collects metrics for all hosts in the specified cluster
- Incrementally stores metrics to JSON files organized by cluster/node/date/hour
- Can output to stdout for debugging or piping to other tools

### Building the Executable

```powershell
# Using PyInstaller
pip install pyinstaller
python -m PyInstaller --onefile collector_cli.py

# The executable will be created at:
# Windows: dist\collector_cli.exe
# Linux/macOS: dist/collector_cli
```

Or use the provided build scripts:

**Windows (PowerShell):**
```powershell
.\build_collector.ps1
```

**Linux/macOS:**
```bash
chmod +x build_collector.sh
./build_collector.sh
```

### Collector CLI Usage

```bash
# Collect metrics using PowerShell provider (dmclient.exe)
collector_cli.exe --cluster MTTitanMetricsBE-Prod-MWHE01 --provider powershell

# Collect metrics using file-based cluster config
collector_cli.exe --cluster my-cluster --provider file --config config/clusters.yaml

# Specify custom dmclient path and machine function
collector_cli.exe --cluster MTTitanMetricsBE-Prod-MWHE01 --provider powershell \
    --dmclient-path "C:\tools\dmclient.exe" --machine-function CH

# Output to stdout instead of log files
collector_cli.exe --cluster my-cluster --provider file --stdout

# Use custom output directory
collector_cli.exe --cluster my-cluster --provider powershell --output-dir /var/log/metrics

# Collect specific metrics only
collector_cli.exe --cluster my-cluster --provider file --metrics cpu_percent,memory_percent

# Verbose output
collector_cli.exe --cluster my-cluster --provider file --verbose
```

### Command Line Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--cluster` | `-c` | (required) | Cluster name to collect metrics for |
| `--provider` | `-p` | `powershell` | Provider type: `powershell` or `file` |
| `--config` | | `config/clusters.yaml` | Path to clusters config file (for file provider) |
| `--dmclient-path` | | `.\dmclient.exe` | Path to dmclient.exe (for powershell provider) |
| `--machine-function` | | `CH` | Machine function filter (for powershell provider) |
| `--output-dir` | `-o` | `data/metrics` | Output directory for metric log files |
| `--stdout` | | | Output metrics to stdout as JSON |
| `--metrics` | `-m` | all | Comma-separated list of metrics to collect |
| `--verbose` | `-v` | | Enable verbose output |

### Provider Types

| Provider | Description |
|----------|-------------|
| `powershell` | Uses dmclient.exe to get cluster info (parses CSV output). Region is auto-extracted from cluster name suffix. |
| `file` | Reads cluster info from YAML/JSON configuration file |

### Cron Job Examples

**Linux crontab:**
```bash
# Collect every minute
* * * * * /opt/healthmonitor/collector_cli -c my-cluster -p file --config /etc/healthmonitor/clusters.yaml -o /var/lib/metrics

# Collect every 5 minutes
*/5 * * * * /opt/healthmonitor/collector_cli -c production-cluster -p file --config /etc/healthmonitor/clusters.yaml
```

**Windows Task Scheduler:**
```
Program: C:\HealthMonitor\dist\collector_cli.exe
Arguments: --cluster my-cluster --provider file --config C:\HealthMonitor\config\clusters.yaml --output-dir C:\HealthMonitor\data\metrics
Trigger: Daily, repeat every 1 minute
```

### Available Metrics

| Metric Name | Unit | Description |
|-------------|------|-------------|
| cpu_percent | % | CPU usage percentage |
| memory_percent | % | Memory usage percentage |
| memory_used | bytes | Used memory in bytes |
| disk_percent | % | Disk usage percentage |
| disk_used | bytes | Used disk space in bytes |
| network_bytes_recv | bytes | Network bytes received |
| network_bytes_sent | bytes | Network bytes sent |
| node_status | - | Node availability (1=up, 0=down) |
| load_average | - | System load average |
| process_count | - | Number of running processes |

## License

MIT License
