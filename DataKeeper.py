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

##############################################################################################################
#                                                Variable
##############################################################################################################
context = zmq.Context()
Download = context.socket(zmq.REP)      # bind
Upload = context.socket(zmq.PULL)       # bind
Alive = context.socket(zmq.PUB)         # connect
Successful = context.socket(zmq.PULL)   # connect

MasterIP = None
MasterPortSub = None
MasterPortSuccessful = None

MyInfo = {}


#############################################################################################################
#                                                  Handlers
############################################################################################################
def Alarm(signum , frame):
    AliveMethod()
    signal.alarm(1)


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

# Successful
def SuccessfulMethod():
    pass


# Estaplish connection
def Connections():
    Alive.connect("tcp://"+MasterIP+":"+MasterPortSub)
    Successful.connect("tcp://"+MasterIP+":"+MasterPortSuccessful)
    Download.bind("tcp://"+MyInfo["IP"]+":"+MyInfo["PortDownload"])
    Upload.bind("tcp://"+MyInfo["IP"]+":"+MyInfo["PortUpload"])


##############################################################################################################
#                                                   Main
##############################################################################################################
if __name__ == "__main__":

    with open('DKConfig.json') as config_file:
        data = json.load(config_file)


    MasterIP = data["MasterIP"]
    MasterPortSub = data["MasterPortSub"]
    MasterPortSuccessful = data["MasterPortSuccessful"]


    MyInfo["message"] = "I'm alive"
    MyInfo["IP"] = sys.argv[1]
    MyInfo["PortDownload"] = sys.argv[2]
    MyInfo["PortUpload"] = sys.argv[3]

    Connections()

    signal.signal(signal.SIGALRM, Alarm)
    signal.alarm(1)



