import zmq
import time
import sys
from zmq import Socket
import pandas as pd
import sysv_ipc as ss
import json
from io import StringIO

#Global Variables
LOOKUP_TABLE=None
memory=None
semaphore=None
portMaster=None
#################

def initMemoryMain():
    global memory
    global semaphore
    global LOOKUP_TABLE
    try:
        memory = ss.SharedMemory(1334, ss.IPC_CREAT)
    except:
        memory = ss.SharedMemory(1334, ss.IPC_CREX)    
    return memory
def initSemaphoreMain():    
    global semaphore    
    try:
        semaphore = ss.Semaphore(1334,ss.IPC_CREAT)
    except:
        semaphore = ss.Semaphore(1334,ss.IPC_CREX)
    return semaphore

def initMemory():
    global memory  
    memory = ss.SharedMemory(1334, ss.IPC_CREAT)
    return memory
def initSemaphore():
    global semaphore  
    semaphore = ss.Semaphore(1334, ss.IPC_CREAT)
    return semaphore



def updateMyLOOKUPTABLE(myNewrow=None):
    global memory
    global semaphore
    global LOOKUP_TABLE
    print('beefore entring the CS')
    semaphore.acquire() 
    print('entring the CS')
    global LOOKUP_TABLE
    mybytes=memory.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    LOOKUP_TABLE=pd.read_csv(TESTDATA,sep=',')
    if myNewrow is not None:
        LOOKUP_TABLE = LOOKUP_TABLE.append(myNewrow,ignore_index=True)
        csvfile=LOOKUP_TABLE.to_csv()
        memory.write(str(csvfile))    
    print('beefore exit the CS')
    semaphore.release()
    print(' exit the CS')

def init():
    global portMaster
    global memory
    global semaphore
    global LOOKUP_TABLE
    portMaster = "5556"
    if(len(sys.argv)>1):
        portMaster=str(sys.argv[1])
    with open('master_tracker_config.json') as f:
        masterInfo = json.load(f)
    if(masterInfo['MASTER_MAIN_PORT']==portMaster):
        LOOKUP_TABLE = pd.DataFrame({'user_id':[1,2,3],'filename':[1,2,3],'node_number':[1,2,3],'path_on_node':[1,2,3],'is_node_alive':[1,2,3]})
        memory=initMemoryMain()
        semaphore=initSemaphoreMain()  
        semaphore.release()
        memory.remove()
        semaphore.remove()    
        memory=initMemoryMain()
        semaphore=initSemaphoreMain() 
        semaphore.release()     
        csvfile=LOOKUP_TABLE.to_csv()        
        memory.write(str(csvfile))
    else:
        memory=initMemory()
        semaphore=initSemaphore()
        mybytes=memory.read()
        s=str(mybytes,'utf-8')
        TESTDATA = StringIO(s)
        LOOKUP_TABLE=pd.read_csv(TESTDATA)
        semaphore.release()


def readSendVideo(Videopath:str,socket:Socket):
    messagevid={}
    with open(Videopath,'rb') as vfile:
        file=vfile.read()
        messagevid['VIDEO']=file
    socket.send_pyobj(messagevid)
    return True

if __name__ == "__main__":      
    init()
    masterContext = zmq.Context()
    masterSocket = masterContext.socket(zmq.REP)
    masterSocket.bind("tcp://*:%s" % portMaster)
    while True:       
        messageClient = masterSocket.recv_pyobj()
        print("Received request: ", messageClient)
        if messageClient['REQ_TYPE']=='upload':     
            sendMessege={'PORT_NUMBER':'6556','IP':'localhost'}
            masterSocket.send_pyobj(sendMessege)
            dkPort='6556'
            dkContext = zmq.Context()
            consumer_receiver = dkContext.socket(zmq.PULL)
            consumer_receiver.bind("tcp://*:6556")
            video=consumer_receiver.recv_pyobj()
            print(video)
        elif messageClient['REQ_TYPE']=='download':
            fileName=messageClient['FILE_NAME']
            #search for dk which has the this file(video)
            sendMessege={'DK_INFO':[('localhost','6557')]}
            masterSocket.send_pyobj(sendMessege)
            dkContext = zmq.Context()
            dk_download = dkContext.socket(zmq.REP)
            dk_download.bind("tcp://*:6557")
            message = dk_download.recv_string()
            print("Received request: ", message) 
            readSendVideo('cat.mp4',dk_download)

