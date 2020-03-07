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


context = zmq.Context()
Upload = context.socket(zmq.PUSH)       # bind
Upload.connect("tcp://192.168.0.103:21212")

DataOfVidoe = {"VIDEO_NAME" : "cat5.mp4"}
Videopath = "cat.mp4"
with open(Videopath,'rb') as vfile:
    file=vfile.read()
    DataOfVidoe["VIDEO"] = file
Upload.send_pyobj(DataOfVidoe)