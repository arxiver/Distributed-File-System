import json
import pandas as pd
import threading
import time
import zmq
import numpy as np
import multiprocessing 
import os
from multiprocessing import Manager

def is_alive_check():
    return 

def replica_check():
    return 

def update_table():
    return 

# dummy test for semaphores and shared mem.
def isalive(_data,lock):
    lock.acquire() 
    _data['0']=0
    lock.release()
def isalive2(x,lock):
    lock.acquire() 
    _data['1']=2
    lock.release()

if (__name__ == '__main__'):
    with open('config.json') as config_file:
        data = json.load(config_file)

    data_nodes = data['DataNodes']
    replica_factor = data['Rfactor']
    #print(data_nodes[0]['IPv4'])
    for node in data_nodes:
        print(node)
    print(replica_factor)
    LOOKUP_TABLE = pd.DataFrame({'user_id':[], 'filename':[], 'node_number':[], 'path_on_node':[], 'is_node_alive':[]})
    print(LOOKUP_TABLE)
 
    man = Manager()
    d = man.dict()

    # creating a lock object 
    lock = multiprocessing.Lock() 

    # creating new processes 
    p1 = multiprocessing.Process(target=isalive, args=(d,lock)) 
    p2 = multiprocessing.Process(target=isalive2, args=(d,lock)) 

    # starting processes 
    p1.start() 
    p2.start() 

    # wait until processes are finished 
    p1.join() 
    print("P1 passed: ",d)
    p2.join() 
    print("P2 passed: ",d)


