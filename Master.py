#!/home/sofyan/anaconda3/bin/python
import zmq
import sys
import os
import time


context = zmq.Context()
Client = context.socket(zmq.SUB)
Client.bind("tcp://127.0.0.1:5000")
Client.subscribe("")

time.sleep(2)
while True:
    message = Client.recv_pyobj()
    print(dict(message))
    # time.sleep(3)

