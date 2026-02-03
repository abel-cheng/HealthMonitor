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

## Standalone Metric Collector

The project includes a standalone metric collector (`collector_cli.py`) that can be compiled into an executable and run independently via cron jobs or Windows Task Scheduler.

### Building the Executable

**Windows (PowerShell):**
```powershell
.\build_collector.ps1
```

**Linux/macOS:**
```bash
chmod +x build_collector.sh
./build_collector.sh
```

The executable will be created at `dist/health_collector` (or `dist\health_collector.exe` on Windows).

### Collector CLI Usage

```bash
# Collect metrics for a specific node
./health_collector --cluster production --node server-01

# Collect metrics for all nodes in clusters config
./health_collector --all --clusters-config config/clusters.yaml

# Output to stdout instead of storing to file
./health_collector --cluster production --node server-01 --stdout

# Use custom storage directory
./health_collector --cluster production --node server-01 --output-dir /var/lib/metrics

# Collect specific metrics only
./health_collector --cluster production --node server-01 --metrics cpu_percent,memory_percent

# Verbose output
./health_collector --cluster production --node server-01 --verbose
```

### Cron Job Examples

**Linux crontab:**
```bash
# Collect every minute
* * * * * /opt/healthmonitor/health_collector -c production -n server-01 -o /var/lib/metrics

# Collect every 5 minutes for all nodes
*/5 * * * * /opt/healthmonitor/health_collector --all --clusters-config /etc/healthmonitor/clusters.yaml -o /var/lib/metrics
```

**Windows Task Scheduler:**
```
Program: C:\HealthMonitor\dist\health_collector.exe
Arguments: --cluster production --node server-01 --output-dir C:\HealthMonitor\data\metrics
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
