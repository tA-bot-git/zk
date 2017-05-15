#!/usr/bin/env python2.7
import time, socket, os, uuid, sys, kazoo, logging, signal, utils
from election import Election
from utils import MASTER_PATH
from utils import TASKS_PATH
from utils import DATA_PATH
from utils import WORKERS_PATH

class Master:
    def __init__(self, zk):
        self.master = False
        self.zk = zk
        self.set_watches()
        print "joining leader election"
        self.election = Election(self.zk, MASTER_PATH, utils.leader_function())

    def assign(self, element):
        if self.election.is_leader:
            workers = self.free_workers(self.zk.getChildren(WORKERS_PATH))
            tasks = self.unassigned_tasks(self.zk.getChildren(WORKERS_PATH), self.zk.getChildren(TASKS_PATH))
            self.assign_tasks_to_workers(workers, tasks)

    def set_watches(self):
        tasks = self.zk.getChildren(TASKS_PATH, watch=self.assign)
        workers = self.zk.getChildren(WORKERS_PATH, watch=self.assign)
        for task in tasks:
            self.zk.get(TASKS_PATH + "/" + task, watch=self.assign)
        for worker in workers:
            self.zk.get(TASKS_PATH + "/" + worker, watch=self.assign)

    def assign_tasks_to_workers(self, workers, tasks):
        print "Assigning tasks to workers"
        for worker in workers:
            self.zk.set(WORKERS_PATH + "/" + worker, tasks.pop())

    def free_workers(self, workers):
        free_workers = []
        for worker in workers:
            if self.zk.get(WORKERS_PATH + "/" + worker)[0] == "idle":
                free_workers.append(worker)
        return free_workers

    def unassigned_tasks(self, workers, tasks):
        for worker in workers:
            if not self.zk.get(WORKERS_PATH + "/" + worker)[0] == "idle":
                tasks.remove(self.zk.get(WORKERS_PATH + "/" + worker)[0])
        return tasks


if __name__ == '__main__':
    zoo_keeper = utils.init()
    master = Master(zoo_keeper)

    while True:
        time.sleep(1)