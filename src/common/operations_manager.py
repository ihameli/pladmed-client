import json
from multiprocessing import Process
from common.operation import Operation
from common.task import Task
from crontab import CronTab
from common.communicator import (
    STOP,
    NEW_DATA,
    IN_PROCESS,
    FINISHED,
    TASK_FINISHED,
    TASK_SENT,
    CREDITS
)
from utils.time_utils import is_over, generate_stoptime
from scripts.measure import main


class OperationsManager():
    def __init__(self, storage, communicator):
        self.storage = storage
        self.communicator = communicator
        self.current_ops = self.storage.read_operations_state()

        self.recover_state()

    def actual_credits(self):
        total_credits = 0

        for operation in self.current_ops.values():
            total_credits += operation.credits

        return total_credits

    def recover_state(self):
        print("From recovering: ", self.current_ops.values())
        for operation in self.current_ops.values():
            if operation.status == IN_PROCESS:
                self.schedule_operation(operation)

        for operation in list(self.current_ops.values()):
            self.check_operation_finished(operation)
        
        for operation in self.current_ops.values():
            finished_tasks = operation.finished_tasks()

            for task in finished_tasks:
                self.communicator.notify_end_task(operation, task)
                    
    def start(self):
        self.current_ops = self.storage.read_operations_state()

        self.p = Process(target=self.run)
        self.p.start()

    def stop(self):
        self.communicator.stop_operations()
        self.p.join()

        print("Operation manager stopped")

    def run(self):
        print("Running with: ", self.current_ops)

        data = self.communicator.read_operations()

        while data[0] != STOP:
            status = data[1]

            if status == CREDITS:
                self.communicator.notify_current_credits(self.actual_credits())
            else:
                operation_data = data[2]
                op_id = operation_data["id"]

                if status == IN_PROCESS:
                    print("New operation event received")
                    operation = Operation(
                        operation_data["id"],
                        operation_data["params"],
                        operation_data["credits"],
                        operation_data["cron"],
                        operation_data["times_per_minute"],
                        operation_data["stop_time"],
                        operation_data["binary"]
                    )

                    self.current_ops[op_id] = operation
                    self.schedule_operation(operation)

                elif status == TASK_FINISHED:
                    print("New task finished event received")
                    task_data = data[3]

                    task = Task(task_data["code"])

                    try:
                        self.current_ops[op_id].add_task(task)
                        self.storage.mark_task_finished(task)
                        self.communicator.notify_end_task(
                            self.current_ops[op_id], task)
                    except:
                        # Operation has been stopped
                        # Clean tmp file
                        print("Removing pending task of finished operation")
                        self.storage.remove_tmp_file(task)
                        pass

                elif status == FINISHED:
                    print("Operation finished")

                    self.current_ops[op_id].status = FINISHED
                    self.check_operation_finished(self.current_ops[op_id])

                elif status == TASK_SENT:
                    print("Task sent event received")
                    task_data = data[3]

                    task = Task(task_data["code"])

                    self.current_ops[op_id].update_task(task, TASK_SENT)
                    # Save so that a sent task doesn't re-send in case of crash
                    self.save_current_status()
                    self.storage.remove_task(task)
                    self.check_operation_finished(self.current_ops[op_id])

                self.save_current_status()

                print("Current ops after: ", self.current_ops)

            data = self.communicator.read_operations()

        print("Operation manager ending its work...")

    def add_operation(self, operation):
        self.communicator.add_operation(operation)

    def check_operation_finished(self, operation):
        if operation.status == FINISHED and operation.all_task_sent():
            data_to_send = {
                "credits": operation.credits
            }

            self.communicator.emit(
                "finish_operation",
                data_to_send
            )

            del self.current_ops[operation.id]

    def save_current_status(self):
        self.storage.save_operations_state(self.current_ops)

    def schedule_operation(self, operation):
        cron_stoptime = generate_stoptime(operation)

        print("Params are: ", operation.params)

        #sub_cmd_str = "|".join([f"'{param}'" for param in operation.params])
        sub_cmd_str = "|".join([f"{param}" for param in operation.params])

        operation_str = json.dumps(operation.data())

        # cron_command = f"python3 /src/scripts/measure.py {operation.times_per_minute} '{sub_cmd_str}' '{operation_str}' '{operation.binary}' >> /src/output.log 2>&1"
        cron_command = f"python3 /src/scripts/measure.py {operation.times_per_minute} '{sub_cmd_str}' '{operation_str}' '{operation.binary}'"

        print("Cron command: ", cron_command)

        #p = Process(target=main)
        # p.start()
        # Saves execution cron
        with CronTab(user=True) as cron:
            job = cron.new(command=cron_command, comment=operation.id)
            # job.minute.every(1)
            job.setall(operation.cron)

            print("Valid job: ", job.is_valid())

        # Saves stopping cron
        with CronTab(user=True) as cron:
            stop_command = f"python3 /src/scripts/stopper.py {operation.id} '{operation_str}'"
            job = cron.new(command=stop_command, comment=operation.id)
            job.setall(cron_stoptime)

        if is_over(operation):
            print("Operation has to stop due to finish date")
            self.cancel_operation(operation)
            return

        print("Jobs all set")

    def remove_cron_task(self, operation):
        print(f"Removing crontab jobs with op_id: ${operation.id}")
        with CronTab(user=True) as cron:
            job_iter = cron.find_comment(operation.id)

            for job in job_iter:
                print(f"Found a contab job to delete ${job}")
                cron.remove(job)

    def cancel_operation(self, operation):
        self.remove_cron_task(operation)
        self.communicator.finish_operation(operation)

