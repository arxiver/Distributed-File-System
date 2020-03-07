#!/home/sofyan/anaconda3/bin/python

''' 
How to run this process
    - ./DataKeeper.py ip PortOfDownload PortOfUpload

    - in config file i need the (ip of master + the port of successful message)

    - in download method 
        - the client will send an dictionary with (VIDEO_NAME) key

    - in upload method 
        - the client will send an dictionary with (VIDEO_NAME , VIDEO)
'''
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
#                                                Variables
##############################################################################################################
context = zmq.Context()
Download = context.socket(zmq.REP)      # bind
Upload = context.socket(zmq.PULL)       # bind
Successful = context.socket(zmq.PUB)    # connect
ReplicateOrder= context.socket(zmq.PULL)

MasterIP = None
PathOfVideos = None
MasterPortSuccessful = None
MasterPortReplicate = None

MyInfo = {}



##############################################################################################################
#                                                  Functions
##############################################################################################################
# Download 
def DownloadMethod():
    while True:
        Message = Download.recv_pyobj()
        File = Message["VIDEO_NAME"]
        Path = PathOfVideos+"/"+File
        with open(Path,'rb') as vfile:
            Vid=vfile.read()
        Download.send_pyobj(Vid)
        SuccessfulMethod("Download")
        print("The client has downloaded a video and the master has been told about that")


# Upload 
def UploadMethod():
    while True:
        DataOfVideo = Upload.recv_pyobj()
        Path=PathOfVideos+"/"+DataOfVideo["VIDEO_NAME"]
        saveVideo(DataOfVideo["VIDEO"],Path)
        SuccessfulMethod("Upload")
        print("The client has uploaded a video  and the master has been told about that")

def SuccessfulMethod(Type):
    Messages = [
            {"DownloadMassage" : "Download done" },
            {"UploadMassage" : "Download done"},
            {"ReplicateMassage" : "Replicate done"}
        ]
    if Type == "Download":
        Object = Messages[0]
    elif Type == "Upload":  
        Object = Messages[1]
    else: 
        Object = Messages[2]
    Successful.send_pyobj(Object)

def ReplicateMethod():
    while True:
        Data = ReplicateOrder.recv_pyobj()
        Video_Name  = Data["VIDEO_NAME"]
        IPMachine = Data["IP"]
        PortMachine = Data["PORT"]

        contextMachine = zmq.Context()
        ReplicateVideo = contextMachine.socket(zmq.PUSH)
        ReplicateVideo.connect("tcp://"+IPMachine+":"+PortMachine)

        Path = PathOfVideos+"/"+Video_Name
        with open(Path,'rb') as vfile:
            Vid=vfile.read()

        MessageSent = {"VIDEO_NAME" : Video_Name , "VIDEO" : Vid}
        ReplicateVideo.send_pyobj(MessageSent)
        SuccessfulMethod("Replicate")


# Save Video
def saveVideo(video,Path:str):    
    try:
        with open(Path,'wb') as myfile:
            myfile.write(video)
        print("I have saved a video")
        return True
    except:
        print("I can't save the video")
        return False


# Estaplish connection
def Connections():
    Download.bind("tcp://"+MyInfo["IP"]+":"+MyInfo["PortDownload"])
    Upload.bind("tcp://"+MyInfo["IP"]+":"+MyInfo["PortUpload"])
    Successful.connect("tcp://"+MasterIP+":"+MasterPortSuccessful)
    ReplicateOrder.connect("tcp://"+MasterIP+":"+MasterPortReplicate)



##############################################################################################################
#                                                   Main
##############################################################################################################
if __name__ == "__main__":


    # Initial values 
    with open('DKConfig.json') as config_file:
        data = json.load(config_file)

    PathOfVideos=data["PathOfVideos"]
    MasterIP = data["MasterIP"]
    MasterPortSuccessful = data["MasterPortSuccessful"]
    MasterPortReplicate = data["MasterPortReplicate"]


    MyInfo["IP"] = sys.argv[1]
    MyInfo["PortDownload"] = sys.argv[2]
    MyInfo["PortUpload"] = sys.argv[3]


    # Estaplish connections
    Connections()


    # Threading 
    DownloadThread = threading.Thread(target=DownloadMethod)
    UploadThread = threading.Thread(target=UploadMethod)
    ReplicateThread = threading.Thread(target=ReplicateMethod)


    # Starting threads
    DownloadThread.start()
    UploadThread.start()
    ReplicateThread.start()
    
    DownloadThread.join()
    UploadThread.join()
    ReplicateThread.join()

    print("i have finished")







