
# importing important libraies
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
from datetime import datetime

#   MY GLOBAL VARAIBELS
LOOKUP_TABLE=None 
sharedMemory=None
semaphore=None
ALIVES=None
replica_factor = 3
replica_period = 3

# end

# initializeation Functions
def initMemory(id:int):
    '''
        initialization of shared memory that will hold the look up table
    '''
    global sharedMemory
    try:
        sharedMemory = ss.SharedMemory(id, ss.IPC_CREAT) #this will look for already created shered memory
    except:
        sharedMemory = ss.SharedMemory(id, ss.IPC_CREX)  #this will create a new one
    return

def initSemaphore(id):
    '''
        initialization of semaphore that will lock the shared memory
    '''
    global semaphore    
    try:
        semaphore = ss.Semaphore(id,ss.IPC_CREAT) #this will look for already created semaphore
    except:
        semaphore = ss.Semaphore(id,ss.IPC_CREX) #this will create a new one
    return

#end initalization


def updateMyLOOKUPTABLE(myNewrow=None,alive_list=None,firstTime=False):
    '''
        this function will do one of two things 
        1-it will insert a new table into the look up table
        2-will updata the alive column when the data keepers teel it to do so ...
    '''
    global LOOKUP_TABLE,semaphore,sharedMemory
    #first aquire the semaphore
    semaphore.acquire()

    if firstTime:
        csvfile=LOOKUP_TABLE.to_csv()
        sharedMemory.write(csvfile)

    #some stupid code to get the sharedMemory and convert it into data frame
    mybytes=sharedMemory.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)
    LOOKUP_TABLE=pd.read_csv(TESTDATA,sep=',')
    
    #end of stupid code

    #insert new row into the LOOKUP_TABLE
    if myNewrow is not None:
        LOOKUP_TABLE = LOOKUP_TABLE.append(myNewrow,ignore_index=True)
        csvfile=LOOKUP_TABLE.to_csv()
        sharedMemory.write(csvfile)   
    
    #list of alive DATA keepers
    if alive_list is not None:
        for i,isalive in enumerate(alive_list):
            LOOKUP_TABLE.loc[LOOKUP_TABLE['ID'] == i,'Alive'] = isalive
            csvfile=LOOKUP_TABLE.to_csv()
            sharedMemory.write(csvfile)
    # last thing is to release the semaphores
    semaphore.release() 
    #print(LOOKUP_TABLE)   
    return






def initLookUpTable(id,data_keepers):
    '''
        this fuction is responsiple to get the LOOK_UP_TABLE ready to be used ISA
    '''     
    #first connect to global variables
    global LOOKUP_TABLE,sharedMemory,semaphore
    #call init memory and semaphores functions
    initMemory(id)
    initSemaphore(id)      
    #if it works dont touch it please 
    sharedMemory.remove()
    semaphore.remove()  
    initMemory(id)
    initSemaphore(id) 
    semaphore.release()  
    #end of bad code
    #initiate the look up table
    LOOKUP_TABLE = pd.DataFrame({'ID':[],'IPv4':[],'DPort':[],'UPort':[],'Alive':[],'Dfree':[],'Ufree':[],'Files':[]})
    for node in data_keepers:
        node["ID"] = int(node["ID"])
        node['Dfree'] = [1] * (len(node['DPort']))
        node['Ufree'] = [1] * (len(node['UPort']))
        #print(type(node['Ufree']))
        node['Files'] = []
        node['Alive'] = 0     
        LOOKUP_TABLE = LOOKUP_TABLE.append(node,ignore_index=True)   
    #write the LOOK UP TABLE into the memory 
    updateMyLOOKUPTABLE(firstTime=True)
    return
    
def alarm(signum,frame):
    '''
    this function will be called every 1 second
    '''
    global LOOKUP_TABLE,sharedMemory,semaphore,ALIVES      
    #list of all Datakeeprs' states (alive or not) (zero or one)
    #zero is dead and one is alive
    localalives = ALIVES
    #print(localalives)
    updateMyLOOKUPTABLE(alive_list=localalives)
    ALIVES = np.zeros(len(LOOKUP_TABLE))
    #print(LOOKUP_TABLE)
    signal.alarm(1)
    return 

def receiveSuccessful(port:str):
    '''
        this function just receives successefull messege from the DATA KEEPER that
        the operation is done then act accourdingly

    '''
    context = zmq.Context()
    socket = context.socket(zmq.PULL)    
    socket.bind("tcp://127.0.0.1:"+str(port))
    #print("Hi")
    while True:
        #print("Before")
        msg = socket.recv_pyobj()
        print("Freeing Port: ",msg)
        freePort(msg)
        print("Has been free : ",msg)
    
def freePort(data):
    global LOOKUP_TABLE,sharedMemory,semaphore
    print(data)
    ipv4 = data['IPv4']
    port = data['Port']
    x = data['Type']
    if (x == 'U'):
        filename = data['Filename']
    semaphore.acquire()
    mybytes=sharedMemory.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    LOOKUP_TABLE = pd.read_csv(TESTDATA,sep=',')
    isfree = json.loads(pd.DataFrame(LOOKUP_TABLE.loc[LOOKUP_TABLE['IPv4']==ipv4])[x+'free'].iloc[0])
    ports =json.loads(pd.DataFrame(LOOKUP_TABLE.loc[LOOKUP_TABLE['IPv4']==ipv4])[x+'Port'].iloc[0])
    for i in range(len(ports)):
        if(int(ports[i]) == int(port)):
            isfree[i] = 1
            
    LOOKUP_TABLE.loc[LOOKUP_TABLE['IPv4']==ipv4,x+'free'] = json.dumps(isfree)
    if (x == 'U'):
        newfiles = json.loads((pd.DataFrame(LOOKUP_TABLE.loc[LOOKUP_TABLE['IPv4']==ipv4]))['Files'].iloc[0])       
        if (filename not in newfiles):
            newfiles.append(filename)
            LOOKUP_TABLE.loc[LOOKUP_TABLE['IPv4']==ipv4,'Files'] = json.dumps(newfiles)
    csvfile=LOOKUP_TABLE.to_csv()
    sharedMemory.write(csvfile)   
    semaphore.release()    
    print(str(datetime.now()))
    print("freeing",data)
    return

def acquire_port(table,ip,x,filename=None,isdist = 0):
    acquired_port = None
    isfree = json.loads(pd.DataFrame(table.loc[table['IPv4']==ip])[x+'free'].iloc[0])
    ports =json.loads(pd.DataFrame(table.loc[table['IPv4']==ip])[x+'Port'].iloc[0])
    for i in range(len(ports)):
        if(isfree[i] == 1):
            isfree[i] = 0
            acquired_port = ports[i]
            break
    table.loc[table['IPv4']==ip,x+'free'] = json.dumps(isfree)
    """"""
    if(x=='U' and isdist):
        newfiles = json.loads((pd.DataFrame(table.loc[table['IPv4']==ip]))['Files'].iloc[0])       
        if (filename not in newfiles):
            newfiles.append(filename)
            table.loc[table['IPv4']==ip,'Files'] = json.dumps(newfiles)
    return table,acquired_port


def unacquire_util(table,ip,port,x):
    isfree = json.loads(pd.DataFrame(table.loc[table['IPv4']==ip])[x+'free'].iloc[0])
    ports =json.loads(pd.DataFrame(table.loc[table['IPv4']==ip])[x+'Port'].iloc[0])
    for i in range(len(ports)):
        if(int(ports[i]) == int(port)):
            isfree[i] = 1
    table.loc[table['IPv4']==ip,x+'free'] = json.dumps(isfree)
    return table
    


def _load_file_nodes(table):
    files = {}
    # Get each file `s nodes (Data keepers ips)
    for i in range(len(table)):
        if (table.iloc[i]['Alive']==1):
            node_files = json.loads(table.iloc[i]['Files'])
            for f in node_files:
                if(f in files.keys()):
                    files[f].append(table.iloc[i]['IPv4'])
                else :
                    files[f] = [table.iloc[i]['IPv4']]
    return files

def _load_lookuptable(mybytes):
    mybytes=sharedMemory.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    table = pd.read_csv(TESTDATA,sep=',')
    return table

def replicaUpdate(replicaPort = "7808",replica_period=3):
    context = zmq.Context()
    replica_socket = context.socket(zmq.PUB)         # connect
    replica_socket.bind("tcp://127.0.0.1:%s"%str(replicaPort))
    while True:
        _utilReplicaUpdate(replica_socket)
        time.sleep(replica_period)

def _utilReplicaUpdate(replica_socket):
    global sharedMemory,semaphore,replica_factor
    semaphore.acquire()
    mybytes=sharedMemory.read()  
    table = _load_lookuptable(mybytes)
    files = _load_file_nodes(table)
    allnodes = list(table[table['Alive']==1]["IPv4"])
    for fkey in files.keys() : 
        file_nodes = files[fkey]
        instance_count = len(file_nodes)
        if instance_count < replica_factor:
            # find machine you can copy from go to each machine has free port to upload
            source_port = None
            source_ip = None
            got_source = 0
            for ip in file_nodes:
                if (got_source == 1):
                    break
                else:
                    table , source_port = acquire_port(table,ip,'U',filename=None,isdist=0)
                    if (source_port != None):
                        source_ip = ip
                        got_source = 1
            if (source_port == None):
                continue 
            source_machine = [source_ip,source_port]
            print("*****************************************")
            dst_nodes = (set(allnodes)-set(file_nodes))
            print("DST_NODES",dst_nodes)
            added_machines = instance_count
            # selectMachineToCopyTo
            dst_machines = []
            for dst_ip in dst_nodes:
                if (replica_factor <= added_machines):
                    break
                table, dst_port = acquire_port(table,dst_ip,'U',fkey,isdist=1)
                if (dst_port != None):
                        added_machines += 1
                        dst_machines.append([dst_ip,dst_port])
            if(len(dst_machines) == 0 ):
                unacquire_util(table,source_ip,source_port,'U')
            else:
                NotifyMachineDataTransfer(source_machine, dst_machines,replica_socket,fkey)
    csvfile=table.to_csv()
    sharedMemory.write(str(csvfile))    
    semaphore.release()
    return 

def NotifyMachineDataTransfer(source_machine, dst_machine,replica_socket,vname):
    msg = {"VIDEO_NAME":vname,"SRC":source_machine,"DST":dst_machine}
    replica_socket.send_pyobj(msg)
    print("Notifiy Replica Orders")
    print(msg)
    return 

def _print_table(period=2):
    global sharedMemory,semaphore,replica_factor
    while True:
        mybytes=sharedMemory.read() 
        table = _load_lookuptable(mybytes)
        print(table)
        time.sleep(period)


if __name__ == "__main__":     
    # first load data from the json file to be used
    with open('master_tracker_config.json') as f:
        configData = json.load(f)
    data_keepers = configData['DataNodes']
    portSuccessefull=configData['PORT_SUCCESSEFULL']
    sharedMemoryID=int(configData['SHARED_MEMORY_ID'])
    portAlive = configData['PORT_ALIVE']
    replica_factor = configData['ReplicaFactor']
    replica_period = configData['ReplicaPeriod']
    replicaPort = configData['ReplicaPort']
    # end of extracting data
    #print(str(portAlive))
    #initializing LOOKUP TABLE
    initLookUpTable(sharedMemoryID,data_keepers)
    
    #define array of zeros to detect dead DATAKEEPER in the future
    ALIVES = np.zeros(len(LOOKUP_TABLE))
    
    #define connection for alive finctionaliteis between Master and DATAKEEPER ...
    context = zmq.Context()
    socket = context.socket(zmq.SUB)    
    socket.bind("tcp://127.0.0.1:%s"%portAlive)
    socket.subscribe("")
    #end 

    # defien a signal alarm for heart beating functionalitiy
    signal.signal(signal.SIGALRM, alarm)
    signal.alarm(1)
    t1 = threading.Thread(target=receiveSuccessful, args=[portSuccessefull]) 
    t1.start()
    t2 = threading.Thread(target=replicaUpdate, args=[replicaPort,replica_period]) 
    t2.start()
    print_period = 3
    t3 = threading.Thread(target=_print_table, args=[print_period]) 
    t3.start()

    i = 0
    while True:
        msg = socket.recv_pyobj()
        ALIVES[int(msg["ID"])] = 1
        print("timestamp : ",i)
        i+=1
    
