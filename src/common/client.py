#!/usr/bin/env python

import urllib.request
import urllib.parse
import json
import subprocess

class Client:
    def __init__(self):
        pass
    
    def fetch_work(self):
        try:
            response = urllib.request.urlopen('http://0.0.0.0:5000/')
            print(json.loads(response.read()))
        except:
            print("Can't reach server")

        process = subprocess.run(["scamper", "-?"])
        #process = subprocess.run(["ls"])
