# -*- coding: utf-8 -*-
from flask import Blueprint,jsonify
from config.config import config
routes = Blueprint('routes', __name__, static_folder=config['app']['folder'],  static_url_path=config['app']['folder'])
from flask_cors import CORS
CORS(routes)
from functions.aggregator import Aggr, Aggr_data, Aggregate_data
from functions.monitoring import Mon
from functions.baser import MongoDB

@routes.route('/system/health', methods=['GET'])
def get_health():
    return jsonify({'status': 'Started'}), 200


@routes.route('/system/upload', methods=['GET'])
def upload():  
    
    Aggr.cpu_aggregate()
    Aggr.cpu_core_aggregate()
    Aggr.mem_aggregate()
    Aggr.mem_aggregate_sum()
    Aggr.disk_aggregate()
    Aggr.disk_aggregate_sum()
    
    return jsonify({"status": "data save in base"}), 200

@routes.route('/system/upload_date/<start>/<end>', methods=['GET'])
def upload_data(start, end):  
    
    Aggr_data.cpu_aggregate(start, end)
    Aggr_data.mem_aggregate(start, end)
    Aggr_data.disk_aggregate(start, end)
    
    return jsonify({"status": "data save in base"}), 200
   
@routes.route('/system/aggregate', methods=['GET'])
def aggregate():  
    
    Aggregate_data.hour_to_day()
    
    return jsonify({"status": "new aggregate state in base"}), 200
    
@routes.route('/system/get_points', methods=['GET'])
def get_points():  
    
    data = MongoDB.select_all("month")
    print(data)
    return jsonify({"data": data})
    
@routes.route('/system/check', methods=['GET'])
def monitoring():  
    data = Mon.get_data()   
    return jsonify({"status": "data save in base"}), 200

@routes.route('/system/month', methods=['GET'])
def to_month():  
    MongoDB.summ_cpu()
    MongoDB.summ_mem()
    MongoDB.summ_disk()
    return jsonify({"status": "month data created"}), 200
    
@routes.route('/system/update_info', methods=['GET'])
def to_etcd():  
    Aggr.etcd_info_check()
    return jsonify({"status": "month data created"}), 200
    
    
@routes.route('/host/<name>', methods=['GET'])
def check_info(name):  
    data = MongoDB.select({"host": name}, "test")
    return jsonify({"status": data}), 200