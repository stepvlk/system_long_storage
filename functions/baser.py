from pymongo import MongoClient
from datetime import datetime
from config.config import config

client_h = MongoClient(f'mongodb://{config['app']['base_user']}:{config['app']['base_pass']}@{config['app']['base_url']}')
db_h = client_h['utilizations']
collection_h = db_h.utilizationing_hour

client_d = MongoClient(f'mongodb://{config['app']['base_user']}:{config['app']['base_pass']}@{config['app']['base_url']}')
db_d = client_d['utilizations']
collection_d = db_d.utilizationing_day

client_m = MongoClient(f'mongodb://{config['app']['base_user']}:{config['app']['base_pass']}@{config['app']['base_url']}')
db_m = client_m['utilizations']
collection_m = db_m.utilizationing_month

client_y = MongoClient(f'mongodb://{config['app']['base_user']}:{config['app']['base_pass']}@{config['app']['base_url']}')
db_y = client_y['utilizations']
collection_y = db_y.utilizationing_year

client_t = MongoClient(f'mongodb://{config['app']['base_user']}:{config['app']['base_pass']}@{config['app']['base_url']}')
db_t = client_t['utilizations']
collection_t = db_t.utilizationing_test

class MongoDB():
    def select(query, name):
        if name == "hour": 
            data = collection_h.find_one(query)
        if name == "day":
            data = collection_d.find_one(query)
        if name == "month":
            data = collection_m.find_one(query)
        if name == "year":
            data = collection_y.find_one(query)
        if name == "test":
            data = collection_t.find_one(query, {"_id":0})
            #print(data)
        return data

    def select_query(query, name):
        if name == "hour": 
            data = collection_h.find(query)
        if name == "day":
            data = collection_d.find(query)
        if name == "month":
            data = collection_m.find(query)
        if name == "year":
            data = collection_y.find(query)
         
        return data

    def select_all(name):
        response = []
        if name == "hour": 
            data = collection_h.find({}, {"_id":0})
            for res in data:
                response.append(res)
        if name == "day":
            data = collection_d.find({}, {"_id":0})
            for res in data:
                response.append(res)
        if name == "month":
            data = collection_m.find({}, {"_id":0})
            for res in data:
                response.append(res)
        if name == "year":
            data = collection_y.find({}, {"_id":0})
            for res in data:
                response.append(res)            
            
        return response

    def insert(query, name):
        if name == "hour":  
            collection_h.insert_one(query)
        if name == "day":
            collection_d.insert_one(query)
        if name == "month":
            collection_m.insert_one(query)
        if name == "year":
            collection_y.insert_one(query)   
        if name == "test":
            collection_t.insert_one(query)        
        return "insert"
        
    def insert_all(query, name):
        if name == "hour":  
            collection_h.insert_many(query)
        if name == "day":
            collection_d.insert_many(query)
        if name == "month":
            collection_m.insert_many(query)
        if name == "year":
            collection_y.insert_many(query) 
        if name == "test":
            collection_t.insert_many(query)
        return "insert"

  
    def delete(query,name):
        if name == "hour":  
            collection_h.delete_one(query)
        if name == "day":
            collection_d.delete_one(query)
        if name == "month":
            collection_m.delete_one(query)
        if name == "year":
            collection_y.delete_one(query)    
        return "delete"
        

    def update(query, name):
        if name == "hour":  
            collection_h.delete_one(query)
        if name == "day":
            collection_d.delete_one(query)
        if name == "month":
            collection_m.delete_one(query)
        if name == "year":
            collection_y.delete_one(query)
        return "update"
    
    def count(query, name):
        if name == "hour":  
            data = collection_h.count_documents(query)
        if name == "day":
            data = collection_d.count_documents(query)
        if name == "month":
            data = collection_m.count_documents(query)
        if name == "year":
            data = collection_y.count_documents(query)
        return data
    
    def summ_cpu():
        summ = 0
        data =  collection_d.find({ "metric": "cpu_usage_active", "month": "05"})
        new_data = {}
        for elem in data:
            elemid = elem["host"]
            value = elem["value"]
            new_data[elemid] = new_data.get(elemid, 0) + float(value)

        resp = []
        ts = int(datetime.now().timestamp())
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        for k,v in new_data.items():
            res = {"metric": "cpu_usage_active", "host": k, "value": v/30, "timestamp": ts, "date": dt, "time": datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S'), "month": "05"}
            resp.append(res)
        
        MongoDB.insert_all(resp, "month")
    
    def summ_mem():
        summ = 0
        data =  collection_d.find({ "metric": "mem_used_percent", "month": "05"})
        new_data = {}
        for elem in data:
            elemid = elem["host"]
            value = elem["value"]
            new_data[elemid] = new_data.get(elemid, 0) + float(value)

        resp = []
        ts = int(datetime.now().timestamp())
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        for k,v in new_data.items():
            res = {"metric": "mem_used_percent", "host": k, "value": v/30, "timestamp": ts, "date": dt, "time": datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S'), "month": "05"}
            resp.append(res)
        
        MongoDB.insert_all(resp, "month")
    
    def summ_disk():
        summ = 0
        data =  collection_d.find({ "metric": "disk_used_percent", "month": "05"})
        new_data = {}
        for elem in data:
            elemid = elem["host"]
            value = elem["value"]
            new_data[elemid] = new_data.get(elemid, 0) + float(value)

        resp = []
        ts = int(datetime.now().timestamp())
        dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
        for k,v in new_data.items():
            res = {"metric": "disk_used_percent", "host": k, "value": v/30, "timestamp": ts, "date": dt, "time": datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S'), "month": "05"}
            resp.append(res)
        
        MongoDB.insert_all(resp, "month")