import json
import os

config = {
    "app": {
        "base_url": os.getenv('BASE_URL', "127.0.0.1:27017"),
        "base_user": os.getenv('BASE_USER', "longer"),
        "base_pass": os.getenv('BASE_URL', "longer"),
        "addres": os.getenv('HOST', "0.0.0.0"),
        "port": os.getenv('PORT', 6880),
        "url_grafana": os.getenv('GRF_URL', "https://<URL>/api/datasources/proxy/<ID_DATASOURCE>/api/v1/query_range?"),
        "folder": os.getenv('FOLDER', "/data/logs/"),
        "logfile": os.getenv('LOGFILE', "/data/logs/utilization.log"),
        "auth_grafana": os.getenv('AUTH_GRF', ""),
        "rule": os.getenv('RULE', "grafana"), #victoriametrics
        "etcd_url":  os.getenv('ETCD', "http://127.0.0.1:2500")
  }}