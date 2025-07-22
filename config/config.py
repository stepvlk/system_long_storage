import os
from datetime import timedelta

config = {
    "app": {
        "base_url": os.getenv('BASE_URL', "127.0.0.1:27017"),
        "base_user": os.getenv('BASE_USER', "longer"),
        "base_pass": os.getenv('BASE_PASS', "longer"),
        "host": os.getenv('HOST', "0.0.0.0"),
        "port": os.getenv('PORT', 6880),
        "url_grafana": os.getenv('GRF_URL', "https://<URL>/api/datasources/proxy/<ID_DATASOURCE>/api/v1/query_range?"),
        "folder": os.getenv('FOLDER', ""),
        "logfile": os.getenv('LOGFILE', "utilization.log"),
        "auth_grafana": os.getenv('AUTH_GRF', ""),
        "rule": os.getenv('RULE', "grafana"),
        "etcd_url": os.getenv('ETCD', "http://127.0.0.1:2500"),
        "debug": os.getenv('DEBUG', False),
        "aggregation_interval": int(os.getenv('AGGREGATION_INTERVAL', 12)),  # hours
        "retention_days": int(os.getenv('RETENTION_DAYS', 30)),
        "frontend": {
            "refresh_interval": 5000,  # ms
            "chart_history": 24  # hours
        }
    }
}

config['app']['aggregation_interval_seconds'] = config['app']['aggregation_interval'] * 3600