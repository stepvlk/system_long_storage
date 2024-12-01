# -*- coding: utf-8 -*-
from config.config import config
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics
from routes.routes import routes
import os
from flask_cors import CORS
log_dir = '/data/logs'
os.chmod(log_dir, 0o777)


app = Flask(__name__, static_folder=config['app']['folder'],  static_url_path=config['app']['folder'])
app.register_blueprint(routes)
metrics = PrometheusMetrics(app)
CORS(app)

if __name__ == '__main__':
    app.run(host=config['app']['host'], port=config['app']['port'], debug=True) 