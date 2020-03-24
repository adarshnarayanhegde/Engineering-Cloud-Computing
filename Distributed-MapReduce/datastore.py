import socket, threading
import re
import os.path
import csv
import math
from struct import unpack,pack
import itertools
import sys
import logging
import json


csv.field_size_limit(sys.maxsize)

lock=threading.Lock()
csv_file="datastore.csv"

#Prasing commandline arguments
configFile=sys.argv[1]
config={}

with open(configFile) as jsonFile:
    config=json.load(jsonFile)


# Creating a log file
logging.basicConfig(filename='datastore.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')


#Splitting and shuffling mapper output
def LoadBalancer(key,map_outputs):
    if key=='MAP_COUNT':
        logging.debug("Splitting and shuffling word-count mapper output")
        input_str=map_outputs.rstrip(',')
        all_words=input_str.split(",")
        unique=sorted(list(set(all_words)))
        word=''
        res=[]
        for i in unique:
            for j in all_words:
                if j==i:
                    word+=j
            res.append(word)
            word=''
        red=10
        div=math.ceil(len(unique)/red)
        balance=[]
        while(len(res)>0):
            if len(res)>div:
                balance.append(res[:div])
                del res[:(div)]
            else:
                balance.append(res[:])
                del res[0:]
        s=''
        s_final=''
        for i in balance:
            for j in i:
                s+="".join(j)
            s_final+=s+','
            s=''
        logging.debug("Splitting and shuffling done")
        return s_final

    if key=='MAP_INVERTED':
        logging.debug("Splitting and shuffling inverted-index mapper output")
        input_data=map_outputs.replace('(','').split(')')
        input_data.pop()
        input_data.sort()
        dict1={}
        for i in input_data:
            i=i.split(':~')
            if i[0] not in dict1:
                dict1[i[0]]=" "+i[1]+" |"
            else:
                dict1[i[0]]+=" "+i[1]+" |"

        n = len(dict1) // 2
        i = iter(dict1.items())

        d1 = dict(itertools.islice(i, n))
        d2 = dict(i)

        res=''
        for i in d1:
            res+="("+i+":"+d1[i]+")"
        res+='~~'
        for i in d2:
            res+="("+i+":"+d2[i]+")"

        logging.debug("Splitting and shuffling done")
        return res



class ProcessRequest:
    def runSet(command):
        attr=command.splitlines()
        param=attr[0].split(' ')
        key=param[1]
        value="".join(attr[1:]).strip()
        dict={}
        f_exst=0
        if os.path.isfile(csv_file):
            f_exst=1
        if f_exst is 1:
            lock.acquire()
            try:
                with open(csv_file, 'r') as csvfile:
                    readCSV = csv.reader(csvfile)
                    for row in readCSV:
                        st=str(row[0])
                        st1=str(row[1])
                        dict[st]=st1
                    if key in dict:
                        var=dict.get(key)
                        var+=value
                        dict[key]=var
                    else:
                        dict[key]=value

                with open(csv_file, 'w') as csvfile:
                    writer = csv.writer(csvfile)
                    for row in dict:
                        writer.writerow([row,dict.get(row)])
                    lock.release()
                    logging.debug("Data Successfully Stored")
                    return "STORED\r\n"

            except IOError:
                lock.release()
                logging.error("Error Reading Datastore File")
                return "ERROR READING\r\n"

        else:
            lock.acquire()
            try:
                with open(csv_file, 'w') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([key,value])
                    lock.release()
                    logging.debug("Data Successfully Stored")
                    return "STORED\r\n"
            except Exception:
                lock.release()
                logging.error("Error Reading Datastore File")
                return "ERROR READING\r\n"

    def runGet(command):
        dict={}
        attr=command.split(' ')
        key=attr[1].strip()
        lock.acquire()
        try:
            with open(csv_file, 'r') as csvfile:
                readCSV = csv.reader(csvfile)
                for row in readCSV:
                    st=row[0]
                    st1=row[1]
                    dict[st]=st1

                val=dict.get(key)
                if val:
                    lock.release()
                    logging.debug("Data Successfully Returned")
                    return "VALUE "+key+" "+str(len(val))+"\r\n"+val+"\r\nEND\r\n"
                lock.release()
                logging.debug("Key Not Found")
                return "KEY NOT FOUND\r\n"

        except IOError:
            lock.release()
            logging.error("Error Reading Datastore File")
            return "ERROR READING\r\n"

    def runShuffle(command):
        dict={}
        attr=command.split(' ')
        key=attr[1].strip()
        f_exst=0
        if os.path.isfile(csv_file):
            f_exst=1
        if f_exst is 1:
            lock.acquire()
            try:
                with open(csv_file, 'r') as csvfile:
                    readCSV = csv.reader(csvfile)
                    for row in readCSV:
                        st=str(row[0])
                        st1=str(row[1])
                        dict[st]=st1
                    if key in dict:
                        shuffled=LoadBalancer(key,dict.get(key))
                        if key=='MAP_COUNT':
                            dict['COUNT_SHUFFLED']=shuffled
                            dict_key='COUNT_SHUFFLED'
                        elif key=='MAP_INVERTED':
                            dict['INVERTED_SHUFFLED']=shuffled
                            dict_key='INVERTED_SHUFFLED'
                with open(csv_file, 'a') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([dict_key,shuffled])
                    lock.release()
                    logging.debug("Shuffle Data stored")
                    return "STORED\r\n"

            except IOError:
                lock.release()
                logging.error("Error Reading Datastore File")
                return "ERROR READING\r\n"

    def runBackup(command):
        try:
            if(os.path.exists("datastore.csv")):
                lock.acquire()
                os.rename("datastore.csv","backup_datastore.csv")
                lock.release()
                logging.debug("Backup Completed")
                return "BACKUP DONE\r\n"
        except:
            logging.error("Backup Error")
            return "BACKUP ERROR\r\n"



class MultiClient(threading.Thread):
    def __init__(self,addr,conn):
        threading.Thread.__init__(self)
        self.addr=addr
        self.conn=conn

    def run(self):
        logging.debug("Connection from : %s"%str(self.addr))
        while True:
            try:
                ln=self.conn.recv(8)
                (length,)=unpack('>Q',ln)
                data=b''
                while len(data)< length:
                    to_Read=length-len(data)
                    data+=self.conn.recv(1024 if to_Read > 1024 else to_Read) #Referred from stackoverflow
                command=str(data.decode())
                set=re.search("set",command)
                get=re.search("get",command)
                shuffle=re.search("shuffle",command)
                backup=re.search("backup",command)

                if set:
                    ret=ProcessRequest.runSet(command)
                    self.conn.send(ret.encode())
                elif get:
                    ret=ProcessRequest.runGet(command)
                    if ret:
                        length=pack('>Q',len(ret.encode()))
                        self.conn.send(length)
                        self.conn.send(ret.encode())
                elif shuffle:
                    ret=ProcessRequest.runShuffle(command)
                    if ret:
                        self.conn.send(ret.encode())
                elif backup:
                    ret=ProcessRequest.runBackup(command)
                    self.conn.send(ret.encode())

            except Exception:
                pass


server_soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
host=config['datastore_ip']
port=int(config['datastore_port'])
server_soc.bind((host,port))

print("Datastore Listening...")
logging.info("Datastore Listening...")
while(True):
    server_soc.listen(1)
    conn,addr=server_soc.accept()
    clientThread = MultiClient(addr, conn)
    clientThread.start()
