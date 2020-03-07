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


# def saveVideo(video,videoName:str):    
#     try:
#         with open(videoName,'wb') as myfile:
#             myfile.write(video)
#         return True
#     except:
#         return False

context = zmq.Context()
reciever = context.socket(zmq.PULL) 
reciever.bind("tcp://192.168.0.103:21212")
while True:
    message = reciever.recv_pyobj()
    print(message)




