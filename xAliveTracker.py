
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

#   MY GLOBAL VARAIBELS
LOOKUP_TABLE=None 
sharedMemory=None
semaphore=None
ALIVES=None
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
    print(LOOKUP_TABLE)   
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
        print(type(node['Ufree']))
        node['Files'] = []
        node['Alive'] = 1        
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
    #print(lu1)
    signal.alarm(1)
    return 

def receiveSuccessful(port:str):
    '''
        this function just receives successefull messege from the DATA KEEPER that
        the operation is done then act accourdingly

    '''
    context = zmq.Context()
    socket = context.socket(zmq.PULL)    
    socket.bind("tcp://127.0.0.1:"+port)
    print("Hi")
    while True:
        print("Before")
        msg = socket.recv_pyobj()
        freePort(msg)
    
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
        newfiles.append(filename)
        LOOKUP_TABLE.loc[LOOKUP_TABLE['IPv4']==ipv4,'Files'] = json.dumps(newfiles)
    csvfile=LOOKUP_TABLE.to_csv()
    sharedMemory.write(csvfile)   
    semaphore.release()    
    return

if __name__ == "__main__":     
    # first load data from the json file to be used
    with open('master_tracker_config.json') as f:
        configData = json.load(f)
    data_keepers = configData['DataNodes']
    portSuccessefull=configData['PORT_SUCCESSEFULL']
    sharedMemoryID=int(configData['SHARED_MEMORY_ID'])
    portAlive = configData['PORT_ALIVE']
    # end of extracting data
    print(str(portAlive))
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

    
    while True:
        msg = socket.recv_pyobj()
        ALIVES[int(msg["ID"])] = 1
        print("%s" % msg)
