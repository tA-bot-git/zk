#!/usr/bin/env python2.7
import time, socket, os, uuid, sys, kazoo, logging, signal, utils, random
from election import Election
from utils import MASTER_PATH
from utils import TASKS_PATH
from utils import DATA_PATH
from utils import WORKERS_PATH


class Worker:

    def __init__(self, zk):
        print "initializing"
        self.zk = zk
        self.uuid = uuid.uuid4()
        print "creating own worker-node"
        self.node = self.zk.create(WORKERS_PATH + "/" + self.uuid, value=b"idle")
        self.zk.get(self.node, watch=self.assignment_change)

    def assignment_change(self, event):
        print "got new assignment"
        task_id = self.zk.get(event.path)[0]
        result = utils.task(self.zk.get(DATA_PATH + "/" + task_id))
        print "returning result"
        self.zk.set(TASKS_PATH + "/" + task_id, value=str(result))
        print "idling"
        self.zk.set(self.node, value=b"idle")
        self.zk.get(self.node, watch=self.assignment_change)

if __name__ == '__main__':
    zoo_keeper = utils.init()
    worker = Worker(zoo_keeper)
    while True:
        time.sleep(1)