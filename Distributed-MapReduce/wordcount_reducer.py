import socket
import multiprocessing
from struct import unpack,pack
from xmlrpc.server import SimpleXMLRPCServer
import time
import logging
import sys,json,os

#Prasing commandline arguments
configFile=sys.argv[1]
config={}

with open(configFile) as jsonFile:
    config=json.load(jsonFile)
host=config['wordcount_reducer_ip']
port=int(config['wordcount_reducer_port'])
datastore_host=config['datastore_ip']
datastore_port=int(config['datastore_port'])

lock=multiprocessing.Lock()
server = SimpleXMLRPCServer((host, port), logRequests=True,allow_none=True)

# Creating a log file
logging.basicConfig(filename='wordcount_reducer.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

def reducer(input_data):
    lock.acquire()
    s=input_data.replace("(","").split(')')
    s=[i for i in s if i!='']
    unique=sorted(list(set(s)))
    res=''
    for i in unique:
        word=i.split(':')
        res+="("+str(word[0])+":"+str(s.count(i))+")"

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")


    req="set WORD_COUNT "+str(len(res))+"\r\n"+str(res)+"\r\n"

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

    req="get COUNT_SHUFFLED\r\n"
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
    input_data=input_data.split(',')
    input_data.pop()
    soc.close()
    return input_data

def word_count_reducer():
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


print("Word Count Reducer Started...")
logging.info("Word Count Reducer Started...")
server.register_function(word_count_reducer)
server.serve_forever()
