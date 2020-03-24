from xmlrpc.server import SimpleXMLRPCServer
import multiprocessing
import os
import socket
from struct import pack,unpack
import re
import time
import logging
import sys
import json

#Prasing commandline arguments
configFile=sys.argv[1]
config={}

with open(configFile) as jsonFile:
    config=json.load(jsonFile)
host=config['wordcount_mapper_ip']
port=int(config['wordcount_mapper_port'])

datastore_host=config['datastore_ip']
datastore_port=int(config['datastore_port'])

lock=multiprocessing.Lock()
server = SimpleXMLRPCServer((host, port), logRequests=True,allow_none=True)

# Creating a log file
logging.basicConfig(filename='wordcount_mapper.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')


def mapper(input_data):
    lock.acquire()
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    logging.debug("Process ID for {0}".format(os.getpid()))
    regex = r'\w+'
    words=re.findall(regex,input_data)
    mp=''
    for i in words:
        mp=mp+"("+str(i)+":"+"1"+")"+","
    req="set MAP_COUNT "+str(len(mp))+"\r\n"+str(mp)+"\r\n"

    logging.debug("Storing Map output from: {0}".format(os.getpid()))
    length=pack('>Q',len(req))
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

    req="get MAP_INPUT\r\n"
    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    ln=soc.recv(8)
    (length,)=unpack('>Q',ln)
    data=b''
    while len(data)< length:
        to_Read=length-len(data)
        data+=soc.recv(1024 if to_Read > 1024 else to_Read) #Referred from stackoverflow

    input_data=data.decode()
    input_data = input_data.split("\n",2)[1]
    logging.debug("Input Data Received")
    input_data=input_data.split('~~')
    input_data.pop()
    soc.close()
    return input_data

def word_count_mapper():
    input_data=getData()
    if input_data:
        p = multiprocessing.Pool(10)
        mp=p.map_async(mapper,input_data)
        p.close()
        while (True):
            if (mp.ready()):
                break
            remain = mp._number_left
            logging.debug("Waiting for %s map tasks to complete..."%str(remain))
            time.sleep(0.5)
        logging.debug("Map Task Completed")
        return "MAP_DONE"


print("Word Count Mapper Started...")
logging.info("Word Count Mapper Started...")
server.register_function(word_count_mapper)
server.serve_forever()

