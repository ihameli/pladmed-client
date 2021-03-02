#!/usr/bin/env python3

from common.client import Client
from common.storage import Storage
import config.connection as config
import timesync.sync as timesync
import socketio
import time
import os
import re
import subprocess
from multiprocessing import Process
from common.operations_manager import OperationsManager
from utils.credits import rates_to_credits

def config_connection(client):
    processes = []

    @client.sio.event
    def connect():
        client.connect()
        
    @client.sio.event
    def connect_error(message):
        print("Conn error: ", message)

    @client.sio.event
    def disconnect():
        # This is only called for the process who contains all the other processes
        client.disconnect()

        print("All processes finished")

    @client.sio.on('traceroute')
    def on_traceroute(data):
        print("Traceroute received")
        client.traceroute(data["_id"], data["params"], data["credits"])

    @client.sio.on('ping')
    def on_ping(data):
        client.ping(data["_id"], data["params"], data["credits"])
        
def connect_to_server(client):
    token = os.getenv('TOKEN', 'token')

    connected = False

    config_connection(client)

    while not connected:
        try:
            client.sio.connect(
                url=config.HOST + "?token=" + token,
                transports='websocket'
            )

            connected = True
        except:
            time.sleep(config.DELAY_BETWEEN_RETRY)

import datetime

def main():
    sio = socketio.Client(engineio_logger=True,
                          reconnection=True, reconnection_attempts=0)

    print("Now is: ", datetime.datetime.now())

    #subprocess.run("crond")

    storage = Storage(
        config.RESULT_FOLDER,
        config.STATE_FILE,
        config.TMP_FOLDER
    )
    
    storage.clean_tmp_folder()
    
    operation_rate = os.getenv("OPERATIONS_RATE")

    [max_rate, unit] = re.findall(r'[A-Za-z]+|\d+', operation_rate)

    # Temporary accept only Kbps
    if unit != "Kbps":
        print("Operation rate is not in Kbps")
        return

    max_credits = rates_to_credits(int(max_rate), unit)

    client = Client(sio, storage, max_credits)

    connect_to_server(client)

    timesync.listen()

if __name__ == "__main__":
    main()
