#!/home/sofyan/anaconda3/bin/python
'''
How to run this process 
    - in the file of DKConfig.json, put the IPs of master and the ports of sub connection    
    which you will publish your messages 

    - ./Alive (ip PortOfDownload PortOfUpload)?
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
#                                                Variable
##############################################################################################################
context = zmq.Context()
Alive = context.socket(zmq.PUB)         # connect


MasterIP = None
MasterPortSub = None

MyInfo = {}

##############################################################################################################
#                                                Connection
##############################################################################################################
def Connection():
    Alive.connect("tcp://"+MasterIP+":"+MasterPortSub)



##############################################################################################################
#                                        Function of connections
##############################################################################################################
def SendingMessage():
    while True:
        Alive.send_pyobj(MyInfo)
        print("i have sent the message")
        time.sleep(1)




##############################################################################################################
#                                                   Main
##############################################################################################################
if __name__ == "__main__":

    # Initial values 
    with open('DKConfig.json') as config_file:
        data = json.load(config_file)


    MasterIP = data["MasterIP"]
    MasterPortSub = data["MasterPortSub"]
    ID = data['ID']
    
    # MyInfo["IP"] = sys.argv[1]
    # MyInfo["PortDownload"] = sys.argv[2]
    # MyInfo["PortUpload"] = sys.argv[3]

    MyInfo["ID"] = ID
    Connection()
    SendingMessage()

