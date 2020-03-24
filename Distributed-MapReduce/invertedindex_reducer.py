import socket
import multiprocessing
import os
lock=multiprocessing.Lock()
from struct import unpack,pack
from collections import Counter
import re
from xmlrpc.server import SimpleXMLRPCServer
import time
import logging
import sys,json

#Prasing commandline arguments
configFile=sys.argv[1]
config={}

with open(configFile) as jsonFile:
    config=json.load(jsonFile)
host=config['inverted_reducer_ip']
port=int(config['inverted_reducer_port'])
datastore_host=config['datastore_ip']
datastore_port=int(config['datastore_port'])


server = SimpleXMLRPCServer((host, port), logRequests=True,allow_none=True   )

# Creating a log file
logging.basicConfig(filename='inverted_reducer.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

def reducer(input_data):
    lock.acquire()
    input_data=input_data.strip()

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req="set INVERTED_INDEX "+str(len(input_data))+"\r\n"+str(input_data)+"\r\n"

    logging.debug("Storing Reduce output from: {0}".format(os.getpid()))
    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    logging.debug("Response from Datastore: %s"%data.decode())
    soc.close()
    lock.release()
    return

def getData():
    logging.debug("Getting Input Data")
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req="get INVERTED_SHUFFLED\r\n"
    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    ln=soc.recv(8)
    (length,)=unpack('>Q',ln)
    data=b''
    while len(data)< length:
        to_Read=length-len(data)
        data+=soc.recv(1024 if to_Read > 1024 else to_Read)

    input_data=data.decode()
    input_data = input_data.split("\n",2)[1]
    logging.debug("Input Data Received")
    input_data=input_data.split('~~')
    soc.close()
    return input_data


def inverted_index_reducer():
    input_data=getData()
    p = multiprocessing.Pool(10)
    rd=p.map_async(reducer,input_data)
    p.close()
    while (True):
        if (rd.ready()):
            break
        remain = rd._number_left
        logging.debug("Waiting for %s reduce tasks to complete..."%str(remain))
        time.sleep(0.5)

    logging.debug("Reduce Task Completed")
    return "REDUCER_DONE"

print("Inverted Index Reducer Started...")
logging.info("Inverted Index Reducer Started...")
server.register_function(inverted_index_reducer)
server.serve_forever()
