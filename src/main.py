#!/usr/bin/env python3

import datetime
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
from multiprocessing import Process


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
        # This is only called for the process which contains all the other processes
        client.disconnect()

        print("All processes finished")

    @client.sio.on('traceroute')
    def on_traceroute(data):
        print("Traceroute received")
        client.traceroute(data["_id"], data["params"], data["credits"])

    @client.sio.on('ping')
    def on_ping(data):
        client.ping(data["_id"], data["params"], data["credits"])
        
    @client.sio.on('dns')
    def on_dns(data):
        client.dns(data["_id"], data["params"], data["credits"])

    @client.sio.on('delete')
    def on_delete(data):
        print(f"Got a delete message with params ${data}")
        client.delete(data["_id"], data["params"], data["credits"])


def connect_to_server(client):
    backend_url = 'wss://' + \
        os.getenv('BACKEND_FQDNS') + "/"

    print("Connecting to: ", backend_url)
    
    token = os.getenv('TOKEN', 'token')

    running = True

    config_connection(client)

    while running:
        try:
            client.start_connection(backend_url + "?token=" + token)
            client.sio.wait()
        except Exception as e:
            print("Reconnecting: ", e)
            time.sleep(config.DELAY_BETWEEN_RETRY)


def start_timesync():
    timesync.listen()


def main():
    sio = socketio.Client(engineio_logger=True, reconnection=False)

    print("Now is: ", datetime.datetime.now())

    storage = Storage(
        config.RESULT_FOLDER,
        config.STATE_FILE,
        config.TMP_FOLDER
    )

    storage.clean_tmp_folder()

    operation_rate = os.getenv("OPERATIONS_RATE")

    [max_rate, unit] = re.findall(r'[A-Za-z]+|\d+', operation_rate)

    # Temporarily accept only Kbps
    if unit != "Kbps":
        print("Operation rate is not in Kbps")
        return

    max_credits = rates_to_credits(int(max_rate), unit)

    client = Client(sio, storage, max_credits)

    timesync_p = Process(target=start_timesync)
    timesync_p.start()

    connect_to_server(client)

    timesync_p.join()


if __name__ == "__main__":
    main()
