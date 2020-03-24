from lxml import etree
import xmlrpc.client as xc
import requests,math
import textwrap
import socket
from struct import pack,unpack
import json
import sys
import logging



# Creating a log file
logging.basicConfig(filename='master.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')



ipConfig=sys.argv[1]
ipconf={}

with open(ipConfig) as jsonFile:
    ipconf=json.load(jsonFile)


datastore_host=ipconf['datastore_ip']
datastore_port=int(ipconf['datastore_port'])

wc_mapper_host=ipconf['wordcount_mapper_ip']
wc_mapper_port=(ipconf['wordcount_mapper_port'])

wc_reducer_host=ipconf['wordcount_reducer_ip']
wc_reducer_port=(ipconf['wordcount_reducer_port'])

in_mapper_host=(ipconf['inverted_mapper_ip'])
in_mapper_port=(ipconf['inverted_mapper_port'])

in_reducer_host=ipconf['inverted_reducer_ip']
in_reducer_port=(ipconf['inverted_reducer_port'])

proxy_mapper_wc = xc.ServerProxy("http://%s:%s"%(wc_mapper_host,wc_mapper_port),allow_none=True)
proxy_reducer_wc = xc.ServerProxy("http://%s:%s"%(wc_reducer_host,wc_reducer_port),allow_none=True)
proxy_mapper_in = xc.ServerProxy("http://%s:%s"%(in_mapper_host,in_mapper_port),allow_none=True)
proxy_reducer_in = xc.ServerProxy("http://%s:%s"%(in_reducer_host,in_reducer_port),allow_none=True)


#Function to validate configurations
def validate_config(config):
    if not config['input_data']:
        logging.error("No input data received")
        exit(0)
    if config['map_fn'] not in ['wordCountMapper','invertedIndexMapper']:
        logging.error("Map Function not supported")
        exit(0)
    if config['reduce_fn'] not in ['wordCountReducer','invertedIndexReducer']:
        logging.error("Reduce Function not supported")
        exit(0)
    if not config['output_location']:
        logging.error("Output location not provided")
        exit(0)
    return


#Prasing commandline arguments
configFile=sys.argv[2]
config={}

with open(configFile) as jsonFile:
    config=json.load(jsonFile)

validate_config(config)


#Function to store input data into key-value store
def store_input(map_fn,input_data):

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req=''
    if map_fn=='wordCountMapper':
        response = requests.get(input_data)
        html = etree.HTML(response.text)
        text = '\n'.join([el.text for el in html.findall('.//p')])
        ln=math.floor(len(text)/10)
        res=textwrap.wrap(text, ln)

        split=''
        for i in res:
            split+=i+'~~'

        req="set"+" "+str("MAP_INPUT")+" "+str(len(split))+"\r\n"+str(split)+"\r\n"

    elif map_fn=='invertedIndexMapper':
        text=''
        links=input_data
        for i in links:
            response = requests.get(i)
            html = etree.HTML(response.text)
            text += '\n'.join([el.text for el in html.findall('.//p')])+"~~"

        req="set"+" "+str("INVERTED_INPUT")+" "+str(len(text))+"\r\n"+str(text)+"\r\n"

    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    data=data.decode().strip()
    soc.close()
    return data


#Function to invoke map tasks
def invoke_map(map_fn):
    if map_fn=='wordCountMapper':
        try:
            print("Running Word Count Mapper...")
            logging.debug("RPC Call to Word Count Mapper")
            map=proxy_mapper_wc.word_count_mapper()
            return map
        except:
            logging.error("RPC Call to Word Count Mapper Failed")
    elif map_fn=='invertedIndexMapper':
        try:
            print("Running Inverted Index Mapper...")
            logging.debug("RPC Call to Inverted Index Mapper")
            map=proxy_mapper_in.inverted_index_mapper()
            return map
        except:
            logging.error("RPC Call to Inverted Index Mapper Failed")


#Function to shuffle map outputs
def invoke_shuffle(reduce_fn):
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    if reduce_fn=='wordCountReducer':
        print("Running Word Count Shuffle Task...")
        logging.debug("Running Word Count Shuffle Task...")
        req="shuffle MAP_COUNT\r\n"
    elif reduce_fn=='invertedIndexReducer':
        print("Running Inverted Index Shuffle Task...")
        logging.debug("Running Inverted Index Shuffle Task...")
        req="shuffle MAP_INVERTED\r\n"

    length=pack('>Q',len(req))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    data=data.decode().strip()
    soc.close()
    return data


#Function to invoke reduce tasks
def invoke_reduce(reduce_fn):
    if reduce_fn=='wordCountReducer':
        try:
            print("Running Word Count Reducer...")
            logging.debug("RPC Call to Word Count Reducer")
            res=proxy_reducer_wc.word_count_reducer()
            return res
        except:
            logging.error("RPC Call to Word Count Reducer Failed")
    elif reduce_fn=='invertedIndexReducer':
        try:
            print("Running Inverted Index Reducer...")
            logging.debug("RPC Call to Inverted Index Reducer")
            res=proxy_reducer_in.inverted_index_reducer()
            return res
        except:
            logging.error("RPC all to Inverted Index Reducer Failed")


#Function to write output onto the locaiton provided
def write_output(reduce_fn,output_location):
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    if reduce_fn=='wordCountReducer':
        req="get WORD_COUNT\r\n"
    elif reduce_fn=='invertedIndexReducer':
        req="get INVERTED_INDEX\r\n"


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
    input_data=input_data.replace('(','').split(')')

    data1=''
    for i in input_data:
        data1+=i+"\n"
    if input_data:
        try:
            fd=open(output_location,'w+')
            if reduce_fn=='wordCountReducer':
                fd.write("WORD_COUNT:\n")
            elif reduce_fn=='invertedIndexReducer':
                fd.write("INVERTED_INDEX:\n")
            fd.write(data1)

        except:
            logging.error("Error writing data")

        print("Map-Reduce task completed. Please check the output location for results")
        logging.info("Map-Reduce successfully Completed")

# Function to Create Backup Datastore
def create_bsackup():
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req="backup\r\n"
    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    logging.debug("Response from Datastore: %s"%data.decode())
    data=data.decode()
    data=data.strip()
    if(data=="BACKUP DONE"):
        return True
    else:
        return False

#Mapreduce interface
def run_mapred(input_data, map_fn, reduce_fn, output_location):
    map=shuffle=res=''
    print("Storing input data...")
    logging.info("Storing input data")
    store=store_input(map_fn,input_data)
    logging.debug("Response for input storage: %s",store)
    if store=='STORED':
        print("Starting Map Task")
        logging.info("Starting Map Task")
        map=invoke_map(map_fn)
        logging.debug("Response from Map Task: %s",map)
    else:
        print("Data not stored!")
        logging.error("Data not stored!")


    if map=='MAP_DONE':
        print("Map Task Completed")
        logging.info("Map Task Completed")
        print("Starting Shuffle Task")
        logging.info("Starting Shuffle Task")
        shuffle=invoke_shuffle(reduce_fn)
        logging.debug("Response from Shuffle Task: %s",shuffle)
    else:
        print("Map Task Failed!")
        logging.error("Map Task Failed!")


    if shuffle=='STORED':
        print("Shuffle Task Completed")
        logging.info("Shuffle Task Completed")
        print("Starting Reduce Task")
        logging.info("Starting Reduce Task")
        res=invoke_reduce(reduce_fn)
        logging.debug("Response from Reduce Task: %s",res)
    else:
        print("Shuffle Task Failed!")
        logging.error("Shuffle Task Failed!")

    if res=='REDUCER_DONE':
        write_output(reduce_fn,output_location)
    else:
        print("Reduce Rask Failed!")
        logging.error("Reduce Task Failed!")

    print("Creating Backup file...")
    bk=create_bsackup()
    if(bk):
        logging.debug("Backup Created")
        print("Backup File Created")
    else:
        logging.error("Backup error")
        print("Backup Creation Failed!")


logging.info("Master Spawned")

run_mapred(config['input_data'],config['map_fn'],config['reduce_fn'],config['output_location'])

