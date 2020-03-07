import zmq
import time
import sys
from zmq import Socket
import pandas as pd
import sysv_ipc as ss
import json
from io import StringIO
import os 
import json
import signal
import threading
import numpy as np


def initMemoryMain(id):
    try:
        memory = ss.SharedMemory(id, ss.IPC_CREAT)
    except:
        memory = ss.SharedMemory(id, ss.IPC_CREX)    
    return memory  

def initSemaphoreMain(id):    
    try:
        semaphore = ss.Semaphore(id,ss.IPC_CREAT)
    except:
        semaphore = ss.Semaphore(id,ss.IPC_CREX)
    return semaphore

def initMemory(id):
    memory = ss.SharedMemory(id, ss.IPC_CREAT)
    return memory

def initSemaphore(id):
    semaphore = ss.Semaphore(id, ss.IPC_CREAT)
    return semaphore

def updateMyLOOKUPTABLE(memory,semaphore,LOOKUP_TABLE,myNewrow=None,alive_list=None):
    semaphore.acquire()
    mybytes=memory.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    LOOKUP_TABLE=pd.read_csv(TESTDATA,sep=',')
    if myNewrow is not None:
        LOOKUP_TABLE = LOOKUP_TABLE.append(myNewrow,ignore_index=True)
        csvfile=LOOKUP_TABLE.to_csv()
        memory.write(str(csvfile))    
    
    if alive_list is not None:
        for i,isalive in enumerate(alive_list):
            LOOKUP_TABLE.loc[LOOKUP_TABLE['ID'] == i,'Alive'] = isalive
            csvfile=LOOKUP_TABLE.to_csv()
            memory.write(str(csvfile))    
    semaphore.release()
    return [LOOKUP_TABLE,memory,semaphore]

def init(id,LU):
    portMaster = "5556"
    LOOKUP_TABLE = LU
    memory=initMemoryMain(id)
    semaphore=initSemaphoreMain(id)  
    semaphore.release()
    memory.remove()
    semaphore.remove()    
    memory=initMemoryMain(id)
    semaphore=initSemaphoreMain(id) 
    semaphore.release()     
    csvfile=LOOKUP_TABLE.to_csv()        
    memory.write(str(csvfile))
    return [LOOKUP_TABLE,memory,semaphore]

def Alarm(signum,frame):
    global lu1,mem1,sem1
    #print("HALLO FROM ALARM")
    global ALIVES
    localalives = ALIVES
    #print(localalives)
    [lu1,mem1,sem1] = updateMyLOOKUPTABLE(mem1,sem1,lu1,myNewrow=None,alive_list=localalives)
    ALIVES = np.zeros(len(lu1))
    #print(lu1)
    signal.alarm(1)
    return 

def ReceiveSuccessful():
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    ALIVE_PORT = "5001"
    socket.bind("tcp://127.0.0.1:5001")
    print("Hi")
    while True:
        print("Before")
        msg = socket.recv_pyobj()
        Free_Port(msg)
    
def Free_Port(data):
    global lu1,mem1,sem1
    ipv4 = data['IPv4']
    port = data['Port']
    x = data['Type']
    if (x == 'U'):
        filename = data['Filename']
    sem1.acquire()
    mybytes=mem1.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    table = pd.read_csv(TESTDATA,sep=',')
    isfree = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))[x+'free'].iloc[0])
    ports = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))[x+'Port'].iloc[0])
    for i in range(len(ports)):
        print(ports[i])
        if(ports[i] == port):
            isfree[i] = 1
    table.loc[table['IPv4']==ipv4,x+'free'] = json.dumps(isfree)
    if (x == 'U'):
        newfiles = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))['Files'].iloc[0])
        newfiles.append(filename)
        table.loc[table['IPv4']==ipv4,'Files'] = json.dumps(newfiles)
    csvfile=table.to_csv()
    mem1.write(str(csvfile))    
    sem1.release()
    print(newfiles)
    return

if __name__ == "__main__":      
    global lu1,mem1,sem1,ALIVES
    lu1 = pd.DataFrame({'ID':[],'IPv4':[],'DPort':[],'UPort':[],'Alive':[],'Dfree':[],'Ufree':[],'Files':[]})
    with open('master_tracker_config.json') as f:
        config_data = json.load(f)
    data_keepers = config_data['DataNodes']
    for node in data_keepers:
        node["ID"] = int(node["ID"])
        node['Dfree'] = [1] * (len(node['DPort']))
        node['Ufree'] = [1] * (len(node['UPort']))
        node['Files'] = []
        node['Alive'] = 1
        #node = pd.Series(node)
        lu1 = lu1.append(node,ignore_index=True)
    ALIVES = np.zeros(len(lu1))
    [lu1,mem1,sem1] = init(1334,lu1)
    
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    ALIVE_PORT = "5000"
    socket.bind("tcp://127.0.0.1:%s"%ALIVE_PORT)
    socket.subscribe("")
    signal.signal(signal.SIGALRM, Alarm)
    signal.alarm(1)
    t1 = threading.Thread(target=ReceiveSuccessful, args=()) 
    t1.start()
    while True:
        msg = socket.recv_pyobj()
        ALIVES[int(msg["ID"])] = 1
        #print("%s" % msg)
