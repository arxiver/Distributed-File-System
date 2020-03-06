#!/home/sofyan/anaconda3/bin/python
#############################################################################################################
#                                                libraries
#############################################################################################################
import sys
import os 
import time 
import zmq
import json
import signal
import threading

##############################################################################################################
#                                                Variable
##############################################################################################################
context = zmq.Context()
Download = context.socket(zmq.REP)      # bind
Upload = context.socket(zmq.PULL)       # bind
Alive = context.socket(zmq.PUB)         # connect

MasterIP = None
MasterPortSub = None

MyInfo = {}


#############################################################################################################
#                                                  Handlers
############################################################################################################
def Alarm(signum , frame):
    AliveMethod()


##############################################################################################################
#                                                  Connections
##############################################################################################################
# Download 
def DownloadMethod():
    pass


# Upload 
def UploadMethod():
    pass 

# I'm Alive 
def AliveMethod():
    Alive.send_pyobj(MyInfo)
    print("i have sent the message")


# Estaplish connection
def Connections():
    print(MasterIP)
    Alive.connect("tcp://"+MasterIP+":"+MasterPortSub)
    Download.bind("tcp://"+MyInfo["IP"]+":"+MyInfo["PortDownload"])
    Upload.bind("tcp://"+MyInfo["IP"]+":"+MyInfo["PortUpload"])


##############################################################################################################
#                                                   Main
##############################################################################################################
if __name__ == "__main__":


    # Initial values 
    with open('DKConfig.json') as config_file:
        data = json.load(config_file)


    MasterIP = data["MasterIP"]
    MasterPortSub = data["MasterPortSub"]


    MyInfo["IP"] = sys.argv[1]
    MyInfo["PortDownload"] = sys.argv[2]
    MyInfo["PortUpload"] = sys.argv[3]


    # Estaplish connections
    Connections()


    # Threading 
    DownloadThread = threading.Thread(target=DownloadMethod)
    UploadThread = threading.Thread(target=UploadMethod)


    # signal alarm
    signal.signal(signal.SIGALRM, Alarm)
    signal.alarm(1)


    # Starting threads
    DownloadThread.start()
    UploadThread.start()
    
    DownloadThread.join()
    UploadThread.join()


    while(True):
        pass
    print("i have finished")







