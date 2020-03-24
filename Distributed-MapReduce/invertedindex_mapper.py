import re,os,sys
from xmlrpc.server import SimpleXMLRPCServer
import multiprocessing
import time
from struct import unpack,pack
import socket
import logging
import sys,json

#Prasing commandline arguments
configFile=sys.argv[1]
config={}

with open(configFile) as jsonFile:
    config=json.load(jsonFile)
host=config['inverted_mapper_ip']
port=int(config['inverted_mapper_port'])
datastore_host=config['datastore_ip']
datastore_port=int(config['datastore_port'])


lock=multiprocessing.Lock()
server = SimpleXMLRPCServer((host, port), logRequests=True,allow_none=True)

# Creating a log file
logging.basicConfig(filename='inverted_mapper.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

def mapper(input_data):
    lock.acquire()
    logging.debug("Process ID for {0}".format(os.getpid()))
    regex = r'\w+'
    words=re.findall(regex,input_data)
    unique=sorted(list(set(words)))
    pd="document_"+str(os.getpid())
    res=''
    for i in unique:
        res+="("+str(i)+":~"+str(pd)+":"+str(words.count(i))+")"

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req="set MAP_INVERTED "+str(len(res))+"\r\n"+str(res)+"\r\n"

    logging.debug("Storing Map output from: {0}".format(os.getpid()))
    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    logging.debug("Response from Datastore: %s"%data.decode())
    soc.close()
    lock.release()
    return



#Getting data
def getData():
    logging.debug("Getting Input Data")
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req="get INVERTED_INPUT\r\n"
    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    ln=soc.recv(8)
    (length,)=unpack('>Q',ln)
    data=b''
    while len(data)< length:
        to_Read=length-len(data)
        data+=soc.recv(1024 if to_Read > 1024 else to_Read) #Referred from Stackoverflow

    input_data=data.decode()
    input_data = input_data.split("\n",2)[1]
    logging.debug("Input Data Received")
    input_data=input_data.split('~~')
    input_data.pop()
    soc.close()
    return input_data

def inverted_index_mapper():
    input_data=getData()
    if input_data:
        p = multiprocessing.Pool(10)
        mp=p.map_async(mapper,input_data)
        p.close()
        while (True):
            if (mp.ready()):
                break
            remain = mp._number_left
            print()
            logging.debug("Waiting for %s map tasks to complete..."%str(remain))
            time.sleep(0.5)
        logging.debug("Map Task Completed")
        return "MAP_DONE"


print("Inverted Index Mapper Started...")
logging.info("Inverted Index  Mapper Started...")
server.register_function(inverted_index_mapper)
server.serve_forever()
