# Collector CLI JSON Output Format

## Overview

`collector_cli.exe` collects ClickHouse cluster node health status and saves the results as JSON files.

## Output Directory Structure

```
D:\ServiceHealthMatrixLogs\
└── <cluster_name>\
    └── <year>\
        └── <month>\
            └── <day>\
                └── ServceLogs_<timestamp>.json
```

### Example Path

```
D:\ServiceHealthMatrixLogs\
└── MTTitanMetricsBE-Prod-MWHE01\
    └── 2026\
        └── 02\
            └── 04\
                └── ServceLogs_202602041234.json
```

## JSON File Format

Each execution of `collector_cli` generates a new JSON file containing the collection results for all nodes in the cluster.

### File Naming Convention

- Format: `ServceLogs_<timestamp>.json`
- Timestamp format: `YYYYMMDDHHMM` (UTC time)
- Example: `ServceLogs_202602041234.json`

### JSON Structure

```json
[
  {
    "clustername": "cluster name",
    "machinename": "node name / hostname",
    "metricname": "metric name",
    "metricvalue": 1,
    "logtime": "YYYY-MM-DDTHH:MM:SS"
  }
]
```

### Field Description

| Field | Type | Description |
|-------|------|-------------|
| `clustername` | string | Cluster name, e.g., `MTTitanMetricsBE-Prod-MWHE01` |
| `machinename` | string | Node name or host address |
| `metricname` | string | Metric name, currently `ch_ping` |
| `metricvalue` | int | Metric value: `1` = healthy, `0` = offline/unreachable |
| `logtime` | string | Collection time (UTC), format `YYYY-MM-DDTHH:MM:SS` |

### Complete Example

```json
[
  {
    "clustername": "MTTitanMetricsBE-Prod-MWHE01",
    "machinename": "node1.example.com",
    "metricname": "ch_ping",
    "metricvalue": 1,
    "logtime": "2026-02-04T12:34:56"
  },
  {
    "clustername": "MTTitanMetricsBE-Prod-MWHE01",
    "machinename": "node2.example.com",
    "metricname": "ch_ping",
    "metricvalue": 1,
    "logtime": "2026-02-04T12:34:57"
  },
  {
    "clustername": "MTTitanMetricsBE-Prod-MWHE01",
    "machinename": "node3.example.com",
    "metricname": "ch_ping",
    "metricvalue": 0,
    "logtime": "2026-02-04T12:34:58"
  }
]
```

## Usage Examples

### Basic Usage

```powershell
# Using file configuration
.\collector_cli.exe --cluster test-cluster --provider file --config .\config\clusters.yaml

# Using PowerShell provider (dmclient)
.\collector_cli.exe --cluster MTTitanMetricsBE-Prod-MWHE01 --provider powershell
```

### Custom Output Directory

```powershell
.\collector_cli.exe --cluster test-cluster --provider file --output-dir E:\logs\metrics
```

### Output to Standard Output (JSON)

```powershell
.\collector_cli.exe --cluster test-cluster --provider file --stdout
```

## Metric Description

### ch_ping

- **Name**: ClickHouse Ping Status
- **Collection Method**: HTTP GET `http://<host>:8123/ping`
- **Return Values**:
  - `1` - Service healthy (returns "Ok.")
  - `0` - Service offline or unreachable

## Notes

1. **Time Format**: All timestamps are in UTC
2. **File Generation**: Each collection generates a new file, does not append to existing files
3. **Auto Directory Creation**: Output directories are created automatically if they don't exist
4. **Single File Per Collection**: Each execution generates one JSON file containing all nodes in the cluster
