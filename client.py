#!/usr/bin/env python2.7
import time, socket, os, uuid, sys, kazoo, logging, signal, utils, random
from election import Election
from utils import MASTER_PATH
from utils import TASKS_PATH
from utils import DATA_PATH
from utils import WORKERS_PATH

class Client:
    def __init__(self, zk):
        self.limit = 5
        self.zk = zk
        self.counter = 0

    def submit_task(self):
        task_id = uuid.uuid4()
        task_data = str(random.randint(5, 20))
        print ("creating task", task_id)
        self.zk.create(TASKS_PATH + "/" + task_id)
        self.zk.create(DATA_PATH + "/" + task_id, value=task_data)
        self.counter += 1
        self.zk.get(TASKS_PATH + "/" + task_id, watch=self.task_completed)

    def task_completed(self, event):
        result = self.zk.get(event.path)[0]
        self.zk.delete(event.path)
        self.zk.delete(event.path.replace(TASKS_PATH, DATA_PATH))
        self.counter -= 1
        print ("task", event.path.replace(TASKS_PATH + "/", ""), "completed")
        return result

    def submit_task_loop(self):
        while True:
            if self.counter <= self.limit:
                self.submit_task()


if __name__ == '__main__':
    zoo_keeper = utils.init()
    client = Client(zoo_keeper)
    client.submit_task_loop()
    while True:
        time.sleep(1)