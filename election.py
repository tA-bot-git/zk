#!/usr/bin/env python2.7
import time, socket, os, uuid, sys, kazoo, logging, signal, inspect
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import KazooException

from utils import MASTER_PATH

class Election:

    def __init__(self, zk, path, func,args = None):
        self.election_path = path
        self.zk = zk
        self.is_leader = False
        if not (inspect.isfunction(func)) and not(inspect.ismethod(func)):
            logging.debug("not a function "+str(func))
            raise SystemError
        self.func = func
        self.zNode = self.zk.create(self.election_path + "/", ephemeral=True, sequence=True)
        self.ballot(self.zk.get_children(self.election_path))

    #Checks if the node is the Master/Leader
    #prints out "I'm the Leader"
    def is_leading(self):
        while True:
            self.func()
            time.sleep(1)

    #perform a vote on the children.
    #checks if it's smallest node, then becomes the leader.
    #if not smallest subscribes to it's predecessor
    def ballot(self,children):
       if self.imSmallest(children):
            self.is_leader = True
            self.is_leading()
       else:
           self.subscribeToPrececessor(children)

    def imSmallest(self, children):
        return self.zNode.replace(self.election_path+"/","") == sorted(children)[0]

    def subscribeToPrececessor(self, children):
        sortedChilden = sorted(children)
        predecessor = sortedChilden[sortedChilden.index(self.zNode.replace(self.election_path+"/",""))-1]
        self.zk.exists(self.election_path + "/" +predecessor, watch=self.notify)

    def notify(self, event):
        self.ballot(self.zk.get_children(self.election_path))

def dummyLeaderFunction():
    print "Im the Leader"

if __name__ == '__main__':
    zkhost = "127.0.0.1:2181" #default ZK host
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
    if len(sys.argv) == 2:
        zkhost=sys.argv[2]
        print("Using ZK at %s"%(zkhost))

    zk = KazooClient(hosts=zkhost)
    zk.start()
   
    Election(zk, MASTER_PATH, dummyLeaderFunction )
   
    while True:
        time.sleep(1)
