#!/usr/bin/python3.6
import requests
import json
from urllib.parse import quote
from config.config import config
from functions.baser import MongoDB, collection_d
from datetime import datetime

import json_logging, logging

# log is initialized without a web framework name
json_logging.init_non_web(enable_json=True)

logger = logging.getLogger("system_logs")
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(filename=config['app']['logfile'], maxBytes=5000000, backupCount=1)
logger.addHandler(handler)

headers =  {"Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": config['app']['auth_grafana']}

masks = []

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
        
    def cpu_aggregate():
        start = int(datetime.now().timestamp())
        end = start - 21600 #43200 #21600
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_cpu = """(avg_over_time(cpu_usage_active{host=~".*.*", cpu="cpu-total"}[6h]))"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_cpu, safe='')}&start={end}&end={start}&step=6h", headers=headers)
        data = promql_request.json()
        for res in data['data']['result']:
            for i in res['values']:
                info = MongoDB.select({"host": res['metric']['host']}, "test")
                try:
                    zone = res['metric']['host'].split('-')[0].upper()
                    state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1], "owner": info['owner'], "email": info['email'], "zone": zone, "stand": info['stand']}
                    logger.info(state)
                except:
                   pass
    
    def cpu_core_aggregate():
        start = int(datetime.now().timestamp())
        end = start - 300
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_cpu = """avg_over_time(count by (host) (cpu_usage_active{host=~'.*.*',cpu=~'cpu[0-9]+'}[5m]))"""  #"""+ mask +"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_cpu, safe='')}&start={end}&end={start}&step=5m", headers=headers)
        data = promql_request.json()
        for res in data['data']['result']:
            for i in res['values']:
                info = MongoDB.select({"host": res['metric']['host']}, "test")
                try:
                    zone = res['metric']['host'].split('-')[0].upper()
                    state = {"metric": "count_cpu_core",  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1], "owner": info['owner'], "email": info['email'], "zone": zone, "stand": info['stand']}
                    logger.info(state)
                except:
                    pass
    
    def mem_aggregate():
        start = int(datetime.now().timestamp())
        end = start - 21600
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_mem = """avg_over_time(mem_used_percent{host=~".*.*"}[6h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_mem, safe='')}&start={end}&end={start}&step=6h", headers=headers)
        data = promql_request.json()
        for res in data['data']['result']:
            for i in res['values']:
                info = MongoDB.select({"host": res['metric']['host']}, "test")
                try:
                    zone = res['metric']['host'].split('-')[0].upper()
                    state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1], "owner": info['owner'], "email": info['email'], "zone": zone, "stand": info['stand']}
                    logger.info(state)
                except:
                    pass

    def mem_aggregate_sum():
        start = int(datetime.now().timestamp())
        end = start - 21600
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_mem = """avg_over_time(mem_total{host=~".*.*"} /1073741824[6h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_mem, safe='')}&start={end}&end={start}&step=6h", headers=headers)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                info = MongoDB.select({"host": res['metric']['host']}, "test")
                try:
                    zone = res['metric']['host'].split('-')[0].upper()
                    state = {"metric": "total_memory",  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1], "owner": info['owner'], "email": info['email'], "zone": zone, "stand": info['stand']}
                    logger.info(state)
                except:
                    pass

    def disk_aggregate():
        start = int(datetime.now().timestamp())
        end = start - 21600
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_disk = """avg_over_time(disk_used_percent{host=~".*.*", path="/data"}[6h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_disk, safe='')}&start={end}&end={start}&step=6h", headers=headers)
        data = promql_request.json()
        for res in data['data']['result']:
            for i in res['values']:
                info = MongoDB.select({"host": res['metric']['host']}, "test")
                try:
                    zone = res['metric']['host'].split('-')[0].upper()
                    state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1], "owner": info['owner'], "email": info['email'], "zone": zone, "stand": info['stand']}
                    logger.info(state)
                except:
                    pass
    
    def disk_aggregate_sum():
        start = int(datetime.now().timestamp())
        end = start - 300
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_disk = """avg_over_time(disk_total{host=~".*.*", path="/data"} /1073741824[5m])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_disk, safe='')}&start={end}&end={start}&step=5m", headers=headers)
        data = promql_request.json()
        for res in data['data']['result']:
            for i in res['values']:
                info = MongoDB.select({"host": res['metric']['host']}, "test")
                try:
                    zone = res['metric']['host'].split('-')[0].upper()
                    state = {"metric": "disk_data_total",  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1], "owner": info['owner'], "email": info['email'], "zone": zone, "stand": info['stand']}
                    logger.info(state)
                except:
                    pass
    
    def time_update():
        data = MongoDB.select_all('day')
        for res in data:
            state = collection_d.update_one(res, {"$push":{"time": datetime.utcfromtimestamp(int(res['timestamp'])).strftime('%Y-%m-%dT%H:%M:%SZ')}})
            print(state)

class Aggr_data():
    def cpu_aggregate(start, end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_cpu = """avg_over_time(cpu_usage_active{host=~".*.*", cpu="cpu-total"}[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_cpu, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        #print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                #logger.info(state)
                print(state)
                #to_mongo.append(state)
        #MongoDB.insert_all(to_mongo, 'test')    
    
    def cpu_aggregate_sum(start, end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_cpu = """avg_over_time(count(cpu_usage_active{host=~".*.*", cpu=~"cpu[0-9]+"}) by (host)[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_cpu, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        #print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": "count_cpu_core",  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                #logger.info(state)
                print(state)
                #to_mongo.append(state)
        #MongoDB.insert_all(to_mongo, 'test')
    
    def mem_aggregate(start, end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_mem = """avg_over_time(mem_used_percent{host=~".*.*"}[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_mem, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        #print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                #logger.info(state)
                print(state)
                #to_mongo.append(state)
        #MongoDB.insert_all(to_mongo, 'test')
    
    def mem_aggregate_sum(start, end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_mem = """avg_over_time(mem_total{host=~".*.*"} /1073741824[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_mem, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        #print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": "total_memory",  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                #logger.info(state)
                print(state)
                #to_mongo.append(state)
        #MongoDB.insert_all(to_mongo, 'test')
    
    def disk_aggregate(start,end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_disk = """avg_over_time(disk_used_percent{host=~".*.*", path="/data"}[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_disk, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        #print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                logger.info(state)
                to_mongo.append(state)
        #MongoDB.insert_all(to_mongo, 'test')
    
    def disk_aggregate_sum(start,end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_disk = """avg_over_time(disk_total{host=~".*.*", path="/data"} /1073741824[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_disk, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        #print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": "disk_data_total",  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                #logger.info(state)
                print(state)
                to_mongo.append(state)
        #MongoDB.insert_all(to_mongo, 'test')
    
    def time_update():
        data = MongoDB.select_all('day')
        for res in data:
            state = collection_d.update_one(res, {"$push":{"time": datetime.utcfromtimestamp(int(res['timestamp'])).strftime('%Y-%m-%dT%H:%M:%SZ')}})
            print(state)

class Aggr_time():


    def cpu_aggregate(start, end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_cpu = """avg_over_time(cpu_usage_active{host=~".*.*", cpu="cpu-total"}[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_cpu, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                logger.info(state)
                to_mongo.append(state)
        MongoDB.insert_all(to_mongo, 'test')    

    def mem_aggregate(start, end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_mem = """avg_over_time(mem_used_percent{host=~".*.*"}[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_mem, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                logger.info(state)
                to_mongo.append(state)
        MongoDB.insert_all(to_mongo, 'test')

    def disk_aggregate(start,end):
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        query_disk = """avg_over_time(disk_used_percent{host=~".*.*", path="/data"}[12h])"""
        promql_request = requests.get(f"{config['app']['url_grafana']}query={quote(query_disk, safe='')}&start={start}&end={end}&step=12h", headers=headers)
        print(promql_request.status_code)
        data = promql_request.json()
        to_mongo = []
        for res in data['data']['result']:
            for i in res['values']:
                state = {"metric": res['metric']['__name__'],  "host": res['metric']['host'], "value": i[1], "timestamp": i[0], "date": dt, "time": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ'), "month": datetime.utcfromtimestamp(int(i[0])).strftime('%Y-%m-%dT%H:%M:%SZ').split('-')[1]}
                logger.info(state)
                to_mongo.append(state)
        MongoDB.insert_all(to_mongo, 'test')
    
    def time_update():
        data = MongoDB.select_all('day')
        for res in data:
            state = collection_d.update_one(res, {"$push":{"time": datetime.utcfromtimestamp(int(res['timestamp'])).strftime('%Y-%m-%dT%H:%M:%SZ')}})
            print(state)


class Aggregate_data():
    def hour_to_day():
        pass
    def day_to_month():
        month = datetime.now().month
        summ = 0
        data = MongoDB.select_query({"month": month})
        for res in data:
            summ += res['value']
        print(summ)
    def month_to_year():
        pass

def upload():
    times = [{'start': 1718719200, 'end': 1718762400}, {'start': 1718762400, 'end': 1718805600}, {'start': 1718805600, 'end': 1718848800}, {'start': 1718848800, 'end': 1718892000}, {'start': 1718892000, 'end': 1718935200}, {'start': 1718935200, 'end': 1718978400}, {'start': 1718978400, 'end': 1719021600}, {'start': 1719021600, 'end': 1719064800}, {'start': 1719064800, 'end': 1719108000}, {'start': 1719108000, 'end': 1719151200}, {'start': 1719151200, 'end': 1719194400}, {'start': 1719194400, 'end': 1719237600}]
    #{'start': 1717164000, 'end': 1717207200}, {'start': 1717207200, 'end': 1717250400}, {'start': 1717250400, 'end': 1717293600}, {'start': 1717293600, 'end': 1717336800}, {'start': 1717336800, 'end': 1717380000}, {'start': 1717380000, 'end': 1717423200}, {'start': 1717423200, 'end': 1717466400}, {'start': 1717466400, 'end': 1717509600}, {'start': 1717509600, 'end': 1717552800}, {'start': 1717552800, 'end': 1717596000}, {'start': 1717596000, 'end': 1717639200}, {'start': 1717639200, 'end': 1717682400}, {'start': 1717682400, 'end': 1717725600}, {'start': 1717725600, 'end': 1717768800}, {'start': 1717768800, 'end': 1717812000}, {'start': 1717812000, 'end': 1717855200}, {'start': 1717855200, 'end': 1717898400}, {'start': 1717898400, 'end': 1717941600}, {'start': 1717941600, 'end': 1717984800}, {'start': 1717984800, 'end': 1718028000}, {'start': 1718028000, 'end': 1718071200}, {'start': 1718071200, 'end': 1718114400}, {'start': 1718114400, 'end': 1718157600}, {'start': 1718157600, 'end': 1718200800}, {'start': 1718200800, 'end': 1718244000}, {'start': 1718244000, 'end': 1718287200}, {'start': 1718287200, 'end': 1718330400}, {'start': 1718330400, 'end': 1718373600}, {'start': 1718373600, 'end': 1718416800}, {'start': 1718416800, 'end': 1718460000}, {'start': 1718460000, 'end': 1718503200}, {'start': 1718503200, 'end': 1718546400}, {'start': 1718546400, 'end': 1718589600}, {'start': 1718589600, 'end': 1718632800}, {'start': 1718632800, 'end': 1718676000}, {'start': 1718676000, 'end': 1718719200}, 
    for res in times:
        Aggr_time.cpu_aggregate(res['start'], res['end'])
        Aggr_time.mem_aggregate(res['start'], res['end'])
        Aggr_time.disk_aggregate(res['start'], res['end'])
    print("FINISH")

def delimetr_time():
    data = []
    time = 1717164000
    for i in range(1,50):
        start = time
        time += 43200
        end = start + 43200
        data.append({"start": start, "end": end})
        if time == 1719237600:
            break
    print(data)