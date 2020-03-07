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

while True:
    Upload.connect("tcp://192.168.0.103:21212")
    Upload.send_pyobj("message from sender")
    # time.sleep(2)