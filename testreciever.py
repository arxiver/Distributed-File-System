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


def saveVideo(video,videoName:str):    
    try:
        with open(videoName,'wb') as myfile:
            myfile.write(video)
        return True
    except:
        return False

DataOfVid = {"VIDEO_NAME":"cat5.mp4"}
context = zmq.Context()
reciever = context.socket(zmq.REQ) 
reciever.connect("tcp://127.0.0.1:6001")
reciever.send_pyobj(DataOfVid)
video=reciever.recv_pyobj()
# print(video)
if(saveVideo(video,"Downloaded"+".mp4")):
    print("The file has been saved")
else : 
    print("The file hasn't been saved")




