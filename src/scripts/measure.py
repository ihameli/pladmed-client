import config.connection as config
from os import path, getenv
from common.task import Task
from common.operation import SCAMPER_BINARY, DIG_BINARY
from multiprocessing.connection import Client
import json
import uuid
import time
import subprocess
import sys
import os

sys.path.append(os.path.abspath(os.path.join('../..', 'src')))


address = ('localhost', int(getenv('FINISH_TASK_PORT')))

#TMP_FOLDER = "../safe_storage/tmp/"


def operation_filename(task):
    file_storage = config.TMP_FOLDER + task.code

    return file_storage


def end_task(operation_str, task, client):
    finished_task_data = {
        "operation": json.loads(operation_str),
        "task": task.data()
    }

    client.send(json.dumps(finished_task_data))


def main():
    client = Client(address)

    times_per_minute = int(sys.argv[1])

    sub_cmd_joined = sys.argv[2]

    sub_cmd = sub_cmd_joined.split("|")

    operation_str = sys.argv[3]

    binary = sys.argv[4]

    run_measurement = run_scamper if binary == SCAMPER_BINARY else run_dig
    for i in range(times_per_minute):
        start = time.time()
        task = Task(str(uuid.uuid4()))

        run_measurement(task, sub_cmd)

        time.sleep(max(60/times_per_minute - int(time.time() - start), 0))
        end_task(operation_str, task, client)

    client.close()


def run_scamper(task, sub_cmd):
    subprocess.run(
        [
            "scamper",
            "-O",
            "warts",
            "-o",
            operation_filename(task),
            "-c"
        ] + sub_cmd
    )


def run_dig(task, sub_cmd):
    subprocess.run(
        [
            "dig",
        ] + sub_cmd + [
            f" > {operation_filename(task)}"
        ]
    )


if __name__ == "__main__":
    main()