import zmq
import time
import sys
from zmq import Socket
import pandas as pd
import sysv_ipc as ss
import json
from io import StringIO

#Global Variables
LOOKUP_TABLE:pd.DataFrame=None
sharedMemory=None
semaphore=None
portMaster=None
#################

def initMemory(id:int):
    global sharedMemory  
    try:
        sharedMemory = ss.SharedMemory(id, ss.IPC_CREAT)
    except:
        print('error while loading the shared memory')
    

def initSemaphore(id:int):
    global semaphore  
    try:
        semaphore = ss.Semaphore(id, ss.IPC_CREAT)
    except:
        print('error while loading the semaphore')
    
def updateMyLOOKUPTABLE(myNewrow=None,alive_list=None):
    '''
        this function will do one of two things 
        1-it will insert a new table into the look up table
        2-will updata the alive column when the data keepers teel it to do so ...
    '''
    global LOOKUP_TABLE,semaphore,sharedMemory
    #first aquire the semaphore
    semaphore.acquire()

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
        sharedMemory.write(str(csvfile))    
    
    #list of alive DATA keepers
    if alive_list is not None:
        for i,isalive in enumerate(alive_list):
            LOOKUP_TABLE.loc[LOOKUP_TABLE['ID'] == i,'Alive'] = isalive
            csvfile=LOOKUP_TABLE.to_csv()
            sharedMemory.write(str(csvfile))   
    # last thing is to release the semaphores
    semaphore.release() 
    #print(LOOKUP_TABLE)   
    return


def init():
    global portMaster,sharedMemory,semaphore,LOOKUP_TABLE
    #initiate the port master number
    portMaster = str(sys.argv[1])
    if(len(sys.argv)>1):
        portMaster=str(sys.argv[1])    
    with open('master_tracker_config.json') as f:
        masterInfo = json.load(f)

    # get the LOOK_UP_TABLE from the shared memory
    shared_memory_id=int(masterInfo['SHARED_MEMORY_ID'])    
    initMemory(shared_memory_id)
    initSemaphore(shared_memory_id)
    semaphore.release()
    updateMyLOOKUPTABLE()
    #print(LOOKUP_TABLE)
    return




def readSendVideo(Videopath:str,socket:Socket):
    messagevid={}
    with open(Videopath,'rb') as vfile:
        file=vfile.read()
        messagevid['VIDEO']=file
    socket.send_pyobj(messagevid)
    return True



#this willl be in the download to require ports of the datakeepers ..
def randPortsAcquire(n_needed=1,x='D',filename='cat.mp4'):   
    global semaphore,sharedMemory,LOOKUP_TABLE
    semaphore.acquire()
    mybytes=sharedMemory.read()
    s=str(mybytes,'utf-8')
    s=s[1:] 
    TESTDATA = StringIO(s)    
    LOOKUP_TABLE = pd.read_csv(TESTDATA,sep=',')
    acq_count = 0
    acquired = []    
    typeOfOperation=x  
    for i in range (LOOKUP_TABLE.shape[0]):
        if (int(LOOKUP_TABLE.iloc[i]['Alive'])==int(1)):
            print('HI IAM ALIVE')
            if(typeOfOperation=='D'):
                fileList=json.loads(LOOKUP_TABLE.iloc[i]['Files'])
                fileExist=False
                for file in fileList:
                    if file == filename:
                        print('Here Iam my old frie')
                        fileExist=True
                if(not fileExist):
                    continue
            ports =json.loads(LOOKUP_TABLE.iloc[i][x+'Port'])
            isfree =json.loads(LOOKUP_TABLE.iloc[i][x+'free'])
            print('MY BEFORE')
            print(isfree)
            for port in range(len(isfree)):                                
                if (acq_count >= n_needed):                    
                    break
                if(int(isfree[port]) == int(1)):
                    print('iam stubid')
                    #acquire and set
                    isfree[port] = 0
                    print('HOOOOLAAA')
                    acquired.append([LOOKUP_TABLE.iloc[i]["IPv4"] ,ports[port]])
                    acq_count += 1
            print(isfree)
            LOOKUP_TABLE.loc[LOOKUP_TABLE['ID']==LOOKUP_TABLE.iloc[i]['ID'],x+'free'] = json.dumps(isfree)
    csvfile=LOOKUP_TABLE.to_csv()
    sharedMemory.write(csvfile)    
    semaphore.release()
    print(acquired)
    return acquired


def port_freeing(data):
    global LOOKUP_TABLE,sharedMemory,semaphore
    ipv4 = data['IPv4']
    port = data['Port']
    x = data['Type']    
    semaphore.acquire() 
    mybytes=sharedMemory.read()
    s=str(mybytes,'utf-8')
    s=s[1:]
    TESTDATA = StringIO(s)    
    table = pd.read_csv(TESTDATA,sep=',')
    isfree = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))[x+'free'].iloc[0])
    ports = json.loads((pd.DataFrame(table.loc[table['IPv4']==ipv4]))[x+'Port'].iloc[0])
    for i in range(len(ports)):        
        if(ports[i] == port):
            isfree[i] = 1
    table.loc[table['IPv4']==ipv4,x+'free'] = json.dumps(isfree)
    csvfile=table.to_csv()
    sharedMemory.write(str(csvfile))    
    semaphore.release()
    return 

if __name__ == "__main__": 
    init()
    masterContext = zmq.Context()
    masterSocket = masterContext.socket(zmq.REP)    
    masterSocket.bind("tcp://*:%s" % portMaster)  
    print(portMaster)
        
    while True:       
        messageClient = masterSocket.recv_pyobj()
        print("Received request: ", messageClient)
        if messageClient['REQ_TYPE']=='upload':   
            freePortsList=randPortsAcquire(x='U',filename='cat.mp4')  
            if len(freePortsList)==0:                
                print('sorry there isnt any avilable portd duo to traffic , try agaian after awhile')
                sendMessege={'STATUS':'fail','PORT_NUMBER':None,'IP':None}
                masterSocket.send_pyobj(sendMessege)   
                continue
            sendMessege={'STATUS':'success','PORT_NUMBER':str(freePortsList[0][1]),'IP':str(str(freePortsList[0][0]))}
            masterSocket.send_pyobj(sendMessege)            
        elif messageClient['REQ_TYPE']=='download':  
            print('Hello Sadness My Dear Old Friend')
            #search for dk which has the this file(video)
            fileName=messageClient['FILE_NAME']            
            freePortsList=randPortsAcquire(1,'D',filename=fileName)  
            print(freePortsList)
            if len(freePortsList)==0:
                print('sorry there isnt any avilable portd duo to traffic , try again after awhile')
                continue  
            sendMessege={'DK_INFO':freePortsList}
            masterSocket.send_pyobj(sendMessege)
            

