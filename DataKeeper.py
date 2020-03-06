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
#                                                Variables
##############################################################################################################
context = zmq.Context()
Download = context.socket(zmq.REP)      # bind
Upload = context.socket(zmq.PULL)       # bind

PathOfVideos = None
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
        print("The client has downloaded a video")


# Upload 
def UploadMethod():
    while True:
        DataOfVideo = Upload.recv_pyobj()
        saveVideo(DataOfVideo["VIDEO"],DataOfVideo["VIDEO_NAME"])
        print("The client has uploaded a video")


# Save Video
def saveVideo(video,VidName:str):    
    try:
        Path=PathOfVideos+"/"+VidName
        with open(Path,'wb') as myfile:
            myfile.write(video)
        print("Done")
        return True
    except:
        print("UnDone")
        return False


# Estaplish connection
def Connections():
    print(MasterIP)
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

    PathOfVideos=data["PathOfVideos"]

    MyInfo["IP"] = sys.argv[1]
    MyInfo["PortDownload"] = sys.argv[2]
    MyInfo["PortUpload"] = sys.argv[3]


    # Estaplish connections
    Connections()


    # Threading 
    DownloadThread = threading.Thread(target=DownloadMethod)
    UploadThread = threading.Thread(target=UploadMethod)


    # Starting threads
    DownloadThread.start()
    UploadThread.start()
    
    DownloadThread.join()
    UploadThread.join()

    print("i have finished")







