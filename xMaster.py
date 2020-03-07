import zmq
import time
import sys
from zmq import Socket
import pandas as pd
import sysv_ipc as ss
import json
from io import StringIO
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
    semaphore.release()
    return LOOKUP_TABLE

def init(id):
    portMaster = "5556"
    if(len(sys.argv)>1):
        portMaster=str(sys.argv[1])
    memory=initMemory(id)
    semaphore=initSemaphore(id)
    mybytes=memory.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)
    LOOKUP_TABLE=pd.read_csv(TESTDATA)
    semaphore.release()
    return [LOOKUP_TABLE,memory,semaphore]

def readSendVideo(Videopath:str,socket:Socket):
    messagevid={}
    with open(Videopath,'rb') as vfile:
        file=vfile.read()
        messagevid['VIDEO']=file
    socket.send_pyobj(messagevid)
    return True

def randports_acquire(n_needed=1,x='D'):
    global lu1,mem1,sem1
    sem1.acquire() 
    mybytes=mem1.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    table = pd.read_csv(TESTDATA,sep=',')
    acq_count = 0
    acquired = []
    for i in range (len(table)):
        if (table.iloc[i]['Alive']==1):
            ports = json.loads(table.iloc[i][x+'Port'])
            isfree = json.loads(table.iloc[i][x+'free'])
            for port in range(len(isfree)):
                if (acq_count >= n_needed):
                    break
                if(isfree[port] == 1):
                    #acquire and set
                    isfree[port] = 0
                    acquired.append([table.iloc[i]["IPv4"] ,ports[port]])
                    acq_count += 1
            table.loc[table['ID']==table.iloc[i]['ID'],x+'free'] = json.dumps(isfree)
    csvfile=table.to_csv()
    mem1.write(str(csvfile))    
    sem1.release()
    return acquired

def port_freeing(data):
    ipv4 = data['IPv4']
    port = data['Port']
    x = data['Type']
    global lu1,mem1,sem1
    sem1.acquire() 
    mybytes=mem1.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    table = pd.read_csv(TESTDATA,sep=',')
    isfree = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))[x+'free'].iloc[0])
    ports = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))[x+'Port'].iloc[0])
    for i in range(len(ports)):
        #print(ports[i])
        if(ports[i] == port):
            isfree[i] = 1
    table.loc[table['IPv4']==ipv4,x+'free'] = json.dumps(isfree)
    csvfile=table.to_csv()
    mem1.write(str(csvfile))    
    sem1.release()
    return 

def port_acquire(data):
    ipv4 = data['IPv4']
    port = data['Port']
    x = data['Type']
    #print(ipv4,port,x)
    global lu1,mem1,sem1
    sem1.acquire() 
    mybytes=mem1.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    table = pd.read_csv(TESTDATA,sep=',')
    isfree = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))[x+'free'].iloc[0])
    ports = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))[x+'Port'].iloc[0])
    for i in range(len(ports)):
        if(ports[i] == port):
            isfree[i] = 0
    table.loc[table['IPv4']==ipv4,x+'free'] = json.dumps(isfree)
    csvfile=table.to_csv()
    mem1.write(str(csvfile))    
    sem1.release()
    return 

def replica_check():
    pass

def upload():
    return randports_acquire(n_needed=1,x='U')

def append_file_todk(data):
    global lu1,mem1,sem1
    ipv4 = data['IPv4']
    port = data['Port']
    filename = data['Filename']
    sem1.acquire() 
    mybytes=mem1.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    table = pd.read_csv(TESTDATA,sep=',')
    newfiles = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))['Files'].iloc[0])
    newfiles.append(filename)
    table.loc[table['IPv4']==ipv4,'Files'] = json.dumps(newfiles)
    csvfile=table.to_csv()
    mem1.write(str(csvfile))    
    sem1.release()
    #print(newfiles)
    return

def download(filename='zmo.mo4'):
    #search for alive maci
    global lu1,mem1,sem1
    sem1.acquire() 
    mybytes=mem1.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    download_from_machine = []
    found = 0
    table = pd.read_csv(TESTDATA,sep=',')
    for i in range(len(table)):
        if (found == 1):
            break
        if (table.iloc[i]['Alive']==1):
            files = json.loads(table.iloc[i]['Files'])
            if (filename in files):
                ports = json.loads(table.iloc[i]['DPort'])
                isfree = json.loads(table.iloc[i]['Dfree'])
                for port in range(len(isfree)):
                    if(isfree[port] == 1):
                        isfree[port] = 0
                        download_from_machine.append([table.iloc[i]["IPv4"] ,ports[port]])
                        found = 1
                        break
    if (found == 1):
        table.loc[table['ID']==table.iloc[i]['ID'],'Dfree'] = json.dumps(isfree)
    csvfile=table.to_csv()
    mem1.write(str(csvfile))    
    sem1.release()
    #print(download_from_machine)
    return download_from_machine

                # get free port for this machine download 

    #return
    #table.loc[table['IPv4']==ipv4,x+'free'] = json.dumps(isfree)
    csvfile=table.to_csv()
    mem1.write(str(csvfile))    
    sem1.release()
    return 


    pass

if __name__ == "__main__":      
    id = 1334
    global lu1,mem1,sem1
    [lu1,mem1,sem1] = init(id)
    masterContext = zmq.Context()
    masterSocket = masterContext.socket(zmq.REP)
    with open('master_tracker_config.json') as f:
        replica_factor = json.load(f)['R']
    portMaster = sys.argv[1]
    #print(replica_factor)
    #masterSocket.bind("tcp://*:%s" % portMaster)
    #randports_acquire()
    #free_test = {'IPv4':'192.168.17.5','Type':'D','Port':6001}
    #port_freeing(free_test)
    #free_test = {'IPv4':'192.168.17.5','Type':'D','Port':6000}
    #port_freeing(free_test)
    append_file_todk({'IPv4':'192.168.17.3','Type':'D','Port':6000,'Filename':'zmq.mp4'})
    i = 0
    #download()
    masterContext = zmq.Context()
    masterSocket = masterContext.socket(zmq.REP)
    masterSocket.bind("tcp://*:%s" % portMaster)
    while True:       
        messageClient = masterSocket.recv_pyobj()
        print(messageClient)
        #print("Received request: ", messageClient)
        if messageClient['REQ_TYPE']=='upload':     
            uploadpath = upload()
            #uploadpath = None
            if len(uploadpath) > 0:
                sendMessege={'PORT_NUMBER':uploadpath[0][1],'IP':uploadpath[0][0]}
                print(sendMessege)
                masterSocket.send_pyobj(sendMessege)
            dkPort='6556'
            dkContext = zmq.Context()
            consumer_receiver = dkContext.socket(zmq.PULL)
            consumer_receiver.bind("tcp://*:6556")
            video=consumer_receiver.recv_pyobj()
            #print(video)
        elif messageClient['REQ_TYPE']=='download':
            fileName=messageClient['FILE_NAME']
            #search for dk which has the this file(video)
            downloadpath = (download(fileName))
            #print(downloadpath)
            if len(downloadpath) > 0:
                sendMessege={'DK_INFO':[(downloadpath[0][0],downloadpath[0][1])]}
                masterSocket.send_pyobj(sendMessege)
            dkContext = zmq.Context()
            dk_download = dkContext.socket(zmq.REP)
            dk_download.bind("tcp://*:6557")
            message = dk_download.recv_string()
            print("Received request: ", message) 
            readSendVideo('cat.mp4',dk_download)

