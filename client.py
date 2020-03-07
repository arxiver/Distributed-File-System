'''
Importing nessecery libraries
'''
import zmq
import sys
from zmq import Socket
import cv2
import json
import random
#############################################################3

def initConnections(ports=['5556','5557','5558'],ip="localhost"):
    '''
    inistantiate the connection with the master tracket ports
    '''
    context = zmq.Context()    
    socket = context.socket(zmq.REQ)
    random.shuffle(ports)
    for port in ports:
        socket.connect ("tcp://"+ip+":%s" % port)
        print(str(port))
    print('Done initializing master client ports')
    return socket

#################################################################

def establishUploadConnection(portNum:str,ip:str):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://"+ip+":%s" % portNum)
    return socket

################################################################

def readSendVideo(Videopath:str,socket:Socket):
    messagevid={}
    with open(Videopath,'rb') as vfile:
        file=vfile.read()
        messagevid['VIDEO']=file
    socket.send_pyobj(messagevid)
    return True

###########################################################
def uploadFile(videoPath:str,socketClientMaster:Socket):
    '''
    steps :
    1-request upload 2-recieve information of the data keeper
    3-establish connection 4-send the video
    '''
    myMessegeSend={'REQ_TYPE':'upload'}    
    socketClientMaster.send_pyobj(myMessegeSend)    
    myMessegeRec = socketClientMaster.recv_pyobj()
    portNumberUpload=myMessegeRec['PORT_NUMBER'] 
    ip=myMessegeRec['IP'] 
    print(myMessegeRec)
    socketClientDataKeeper=establishUploadConnection(portNumberUpload,ip) 
    readSendVideo(videoPath,socketClientDataKeeper)
#################################################################  

def saveVideo(video,videoName:str):    
    try:
        with open(videoName,'wb') as myfile:
            myfile.write(video)
        return True
    except:
        return False
    

    
####################################################################
def downloadFile(fileName:str,masterSocket:Socket):
    myMessegeSend={'REQ_TYPE':'download','FILE_NAME':fileName}   
    masterSocket.send_pyobj(myMessegeSend)  
    listMachines=masterSocket.recv_pyobj()
    print(listMachines)
    
    context = zmq.Context()    
    downloadSocketDk = context.socket(zmq.REQ)  
    for IP,port in listMachines['DK_INFO']:
        print(IP,port)
        downloadSocketDk.connect ("tcp://"+str(IP)+":%s"%port)        
        break    
    downloadSocketDk.send_string('give me my video you crazy datakeeper')
    video=downloadSocketDk.recv_pyobj()
    print(video)
    #saveVideo(video,fileName+'1')
    
#######################################################################   

if __name__ == "__main__":  
    #uploadSocket=None 
    with open('master_tracker_config.json') as f:
        masterInfo = json.load(f)
        print(masterInfo)
    masterSocket=initConnections(masterInfo['PortNumbers'],masterInfo['IPv4']) 
    typeOfOperation='u'
    fileName='cat.mp4'
    if(len(sys.argv)>1):
        typeOfOperation=str(sys.argv[1])
    if(len(sys.argv)>2):
        fileName=str(sys.argv[2])
    if typeOfOperation=='u':
        uploadFile(fileName,masterSocket)
    elif typeOfOperation=='d':
        downloadFile(fileName,masterSocket)       
    
   
   
        
        
        






    
