# -*- coding: utf-8 -*-
from config.config import config
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics
from routes.routes import routes
import os
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from functions.aggregator import Aggregate_data

log_dir = '/data/logs'
os.makedirs(log_dir, exist_ok=True)
os.chmod(log_dir, 0o777)

app = Flask(__name__, static_folder=config['app']['folder'], static_url_path=config['app']['folder'])
app.register_blueprint(routes)
metrics = PrometheusMetrics(app)
CORS(app)

scheduler = BackgroundScheduler()
scheduler.add_job(
    func=Aggregate_data.hour_to_day,
    trigger='interval',
    hours=config['app']['aggregation_interval'],
    id='aggregation_job'
)
scheduler.start()

@app.teardown_appcontext
def shutdown_scheduler(exception=None):
    if scheduler.running:
        scheduler.shutdown()

if __name__ == '__main__':
    app.run(host=config['app']['host'], port=config['app']['port'], debug=config['app']['debug'])