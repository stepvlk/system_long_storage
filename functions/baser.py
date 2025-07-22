from pymongo import MongoClient
from datetime import datetime
from config.config import config
from pymongo.errors import PyMongoError

class MongoDBConnection:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            connection_string = f"mongodb://{config['app']['base_user']}:{config['app']['base_pass']}@{config['app']['base_url']}"
            cls._instance.client = MongoClient(
                connection_string,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                serverSelectionTimeoutMS=30000
            )
        return cls._instance
    
    def get_db(self, db_name):
        return self.client[db_name]

class MongoDB:
    _db = MongoDBConnection().get_db('utilizations')
    
    @classmethod
    def _get_collection(cls, name):
        return {
            "hour": cls._db.utilizationing_hour,
            "day": cls._db.utilizationing_day,
            "month": cls._db.utilizationing_month,
            "year": cls._db.utilizationing_year,
            "test": cls._db.utilizationing_test
        }.get(name, cls._db.utilizationing_test)
    
    @classmethod
    def select(cls, query, name):
        try:
            return cls._get_collection(name).find_one(query, {"_id": 0})
        except PyMongoError as e:
            print(f"MongoDB select error: {str(e)}")
            return None
    
    @classmethod
    def select_query(cls, query, name="hour"):
        try:
            return cls._get_collection(name).find(query)
        except PyMongoError as e:
            print(f"MongoDB select_query error: {str(e)}")
            return []
    
    @classmethod
    def select_all(cls, name):
        try:
            return list(cls._get_collection(name).find({}, {"_id": 0}))
        except PyMongoError as e:
            print(f"MongoDB select_all error: {str(e)}")
            return []
    
    @classmethod
    def insert(cls, query, name):
        try:
            return cls._get_collection(name).insert_one(query)
        except PyMongoError as e:
            print(f"MongoDB insert error: {str(e)}")
            return None
    
    @classmethod
    def insert_all(cls, query, name):
        try:
            if not query:
                return None
            return cls._get_collection(name).insert_many(query)
        except PyMongoError as e:
            print(f"MongoDB insert_all error: {str(e)}")
            return None
    
    @classmethod
    def delete(cls, query, name):
        try:
            return cls._get_collection(name).delete_one(query)
        except PyMongoError as e:
            print(f"MongoDB delete error: {str(e)}")
            return None
    
    @classmethod
    def update(cls, query, name):
        try:
            return cls._get_collection(name).update_one(query)
        except PyMongoError as e:
            print(f"MongoDB update error: {str(e)}")
            return None
    
    @classmethod
    def count(cls, query, name):
        try:
            return cls._get_collection(name).count_documents(query)
        except PyMongoError as e:
            print(f"MongoDB count error: {str(e)}")
            return 0
    
    @classmethod
    def summ_cpu(cls):
        return cls._aggregate_metric("cpu_usage_active")
    
    @classmethod
    def summ_mem(cls):
        return cls._aggregate_metric("mem_used_percent")
    
    @classmethod
    def summ_disk(cls):
        return cls._aggregate_metric("disk_used_percent")
    
    @classmethod
    def _aggregate_metric(cls, metric_name):
        try:
            current_month = datetime.now().strftime('%m')
            data = cls._get_collection("day").find({ "metric": metric_name, "month": current_month})
            
            new_data = defaultdict(list)
            for elem in data:
                host = elem["host"]
                new_data[host].append(float(elem["value"]))
            
            resp = []
            ts = int(datetime.now().timestamp())
            dt = datetime.utcnow().strftime("%d %B %Y %I:%M%p")
            
            for host, values in new_data.items():
                avg_value = sum(values) / len(values) if values else 0
                res = {
                    "metric": metric_name,
                    "host": host,
                    "value": avg_value,
                    "timestamp": ts,
                    "date": dt,
                    "time": datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),
                    "month": current_month
                }
                resp.append(res)
            
            if resp:
                cls.insert_all(resp, "month")
            
            return len(resp)
        except PyMongoError as e:
            print(f"MongoDB _aggregate_metric error: {str(e)}")
            return 0