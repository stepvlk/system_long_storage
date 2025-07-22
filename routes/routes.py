# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, render_template
from config.config import config
from functions.aggregator import Aggr, Aggregate_data
from functions.baser import MongoDB
import functions.monitoring as Mon 
from datetime import datetime
import time

routes = Blueprint('routes', __name__, 
                  static_folder=config['app']['folder'], 
                  static_url_path=config['app']['folder'],
                  template_folder='../templates')

@routes.route('/system/health', methods=['GET'])
def get_health():
    return jsonify({'status': 'Started'}), 200

@routes.route('/', methods=['GET'])
def dashboard():
    return render_template('dashboard.html', config=config)

@routes.route('/system/metrics', methods=['GET'])
def get_metrics():
    metrics_data = MongoDB.select({"type": "aggregation_metrics"}, "test") or {}
    
    return jsonify({
        "last_execution": metrics_data.get("last_execution"),
        "execution_time": metrics_data.get("execution_time"),
        "metrics_processed": metrics_data.get("metrics_processed"),
        "success_rate": metrics_data.get("success_rate"),
        "metrics_history": metrics_data.get("metrics_history", []),
        "duration_history": metrics_data.get("duration_history", [])
    })

@routes.route('/system/aggregate', methods=['GET'])
def aggregate():  
    start_time = time.time()
    result = Aggregate_data.hour_to_day()
    execution_time = time.time() - start_time
    
    # Return execution details
    return jsonify({
        "status": "aggregation completed",
        "execution_time": execution_time,
        "metrics_processed": result.get("count", 0),
        "success": result.get("success", False)
    }), 200
    
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