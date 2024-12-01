#!/usr/bin/python3.6
import requests
import json
from urllib.parse import quote
from config.config import config
from functions.baser import MongoDB,collection_d
from datetime import datetime

headers =  {"Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": config['app']['auth_grafana']}


class Mon():
    def get_data():
        data = {
    "results": {
        "test": {
            "status": 200,
            "frames": [
                {
                    "schema": {
                        "name": "1+1",
                        "refId": "test",
                        "meta": {
                            "type": "timeseries-multi",
                            "custom": {
                                "resultType": "vector"
                            },
                            "executedQueryString": "Expr: 1+1\nStep: 1m0s"
                        },
                        "fields": [
                            {
                                "name": "Time",
                                "type": "time",
                                "typeInfo": {
                                    "frame": "time.Time"
                                },
                                "config": {
                                    "interval": 60000
                                }
                            },
                            {
                                "name": "Value",
                                "type": "number",
                                "typeInfo": {
                                    "frame": "float64"
                                },
                                "labels": {},
                                "config": {
                                    "displayNameFromDS": "1+1"
                                }
                            }
                        ]
                    },
                    "data": {
                        "values": [
                            [
                                1717321865161
                            ],
                            [
                                2
                            ]
                        ]
                    }
                }
            ]
        }
    }
}