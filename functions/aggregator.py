#!/usr/bin/python3.6
import requests
import json
from urllib.parse import quote
from config.config import config
from functions.baser import MongoDB
from datetime import datetime, timedelta
import json_logging, logging
import time
from collections import defaultdict

# Initialize logging
json_logging.init_non_web(enable_json=True)
logger = logging.getLogger("system_logs")
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
    filename=config['app']['logfile'], 
    maxBytes=5000000, 
    backupCount=1
)
logger.addHandler(handler)

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": config['app']['auth_grafana']
}

class AggregatorBase:
    @staticmethod
    def _log_metric(metric_data):
        try:
            logger.info(metric_data)
            return True
        except Exception as e:
            logger.error(f"Failed to log metric: {str(e)}")
            return False

    @staticmethod
    def _get_host_info(host):
        try:
            info = MongoDB.select({"host": host}, "test")
            zone = host.split('-')[0].upper()
            return {
                "owner": info.get('owner', 'unknown'),
                "email": info.get('email', 'unknown'),
                "zone": zone,
                "stand": info.get('stand', 'unknown')
            }
        except:
            return {
                "owner": "unknown",
                "email": "unknown",
                "zone": host.split('-')[0].upper(),
                "stand": "unknown"
            }

class Aggr():
    def etcd_info_check():
        start = int(datetime.now().timestamp())
        end = start - 3600
        to_mongo = []
        query_cpu = """(avg_over_time(cpu_usage_active{host=~".*.*", cpu="cpu-total"}[1h]))"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_cpu, safe='')}&start={end}&end={start}&step=1h", headers=headers)
        data = promql_request.json()

        for res in data['data']['result']:
            info = requests.get(f"{config['etcd_url']}/v2/keys/ps/hosts/{res['metric']['host'].split('-')[0]}/{res['metric']['host']}/info")
            resp = info.json()
            if info.status_code != 404:
                resp = info.json()
                req = json.loads(resp['node']['value'])
                tu = req['tu']
                tu_email = req['tu_email']
                stand = req['tags']['place']
                zone = res['metric']['host'].split('-')[0].upper()
            else:
                tu = "default"
                tu_email = "default"
                stand = "default"
                zone = res['metric']['host'].split('-')[0].upper()
            hs = {"host": res['metric']['host'], "tu": tu, "tu_email": tu_email, "stand": stand, "zone": zone}
            to_mongo.append(hs)
        MongoDB.insert_all(to_mongo, 'test')

class Aggregate_data(AggregatorBase):
    @classmethod
    def hour_to_day(cls):
        start_time = time.time()
        metrics_processed = 0
        success_count = 0
        
        try:
            end_time = datetime.now()
            start_time_24h = end_time - timedelta(hours=24)
            

            metric_types = ['cpu_usage_active', 'mem_used_percent', 'disk_used_percent']
            results = []
            
            for metric in metric_types:
                query = f'avg_over_time({metric}{{host=~".*.*"}}[24h])'
                promql_request = requests.get(
                    f"{config['app']['url_grafana']}query={quote(query, safe='')}"
                    f"&start={int(start_time_24h.timestamp())}"
                    f"&end={int(end_time.timestamp())}"
                    f"&step=24h",
                    headers=headers
                )
                
                if promql_request.status_code != 200:
                    continue
                    
                data = promql_request.json()
                
                for res in data['data']['result']:
                    for value in res['values']:
                        host_info = cls._get_host_info(res['metric']['host'])
                        metric_data = {
                            "metric": res['metric']['__name__'],
                            "host": res['metric']['host'],
                            "value": float(value[1]),
                            "timestamp": value[0],
                            "date": end_time.strftime("%d %B %Y %I:%M%p"),
                            "time": datetime.utcfromtimestamp(int(value[0])).strftime('%Y-%m-%dT%H:%M:%SZ'),
                            "month": datetime.utcfromtimestamp(int(value[0])).strftime('%m'),
                            **host_info
                        }
                        
                        if cls._log_metric(metric_data):
                            success_count += 1
                        metrics_processed += 1
                        results.append(metric_data)

            success_rate = success_count / metrics_processed if metrics_processed > 0 else 0

            execution_time = time.time() - start_time
            metrics_history = cls._update_metrics_history(metrics_processed, success_rate, execution_time)
            
            return {
                "count": metrics_processed,
                "success": True,
                "success_rate": success_rate,
                "execution_time": execution_time
            }
            
        except Exception as e:
            logger.error(f"Aggregation failed: {str(e)}")
            return {
                "count": metrics_processed,
                "success": False,
                "error": str(e)
            }

    @classmethod
    def _update_metrics_history(cls, count, success_rate, execution_time):
        try:
            now = datetime.now()
            timestamp = int(now.timestamp())

            metrics = MongoDB.select({"type": "aggregation_metrics"}, "test") or {
                "type": "aggregation_metrics",
                "metrics_history": [],
                "duration_history": []
            }

            metrics.update({
                "last_execution": now.isoformat(),
                "execution_time": execution_time,
                "metrics_processed": count,
                "success_rate": success_rate,
                "last_updated": timestamp
            })

            metrics["metrics_history"].append({
                "x": timestamp * 1000, 
                "y": count
            })
            
            metrics["duration_history"].append({
                "x": timestamp * 1000,
                "y": execution_time
            })
            
            cutoff = (now - timedelta(hours=config['app']['frontend']['chart_history'])).timestamp() * 1000
            metrics["metrics_history"] = [m for m in metrics["metrics_history"] if m["x"] >= cutoff]
            metrics["duration_history"] = [m for m in metrics["duration_history"] if m["x"] >= cutoff]
            
            MongoDB.delete({"type": "aggregation_metrics"}, "test")
            MongoDB.insert(metrics, "test")
            
            return metrics["metrics_history"]
            
        except Exception as e:
            logger.error(f"Failed to update metrics history: {str(e)}")
            return []
        
