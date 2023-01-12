import json,socket, logging, threading, traceback, gc
from datetime import datetime
import requests
import zmq,time,base64
from PyQt5 import QtCore
from threading import Thread
from collections import deque

log_file_format = "[%(levelname)s] - %(asctime)s - %(name)s - : %(message)s in %(pathname)s:%(lineno)d"
log_console_format = "[%(levelname)s] - %(asctime)s - %(pathname)s:%(lineno)d : %(message)s"
main_logger = logging.getLogger()
main_logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter(log_console_format))
main_logger.addHandler(console_handler)
data_queue = deque(maxlen=3)

context = zmq.Context(7)
subSocks = []
stopped=True
upload_config={}

def subscribe(command):
    logging.warning('Not support subscribe!')
    return 0

def doSend():
    global stopped
    global data_queue
    global upload_config
    while not stopped:
        try:
            data,msg_context = data_queue.pop()
        except IndexError:
            time.sleep(0.1)
            continue
        begin_time = datetime.now()
        invoke_response = requests.post(upload_config['url'], files={"filename": data})
        invoke_result = invoke_response.text
        end_time = datetime.now()
        upload_cost = end_time - begin_time
        logging.debug("Upload file cost {} ms".format(upload_cost.microseconds / 1000))
        logging.debug(invoke_result)
        # logging("Upload result: {}".format(upload_result))
        msg = dict()
        for field in msg_context:
            msg[field] = msg_context.get(field)
        msg['result'] = invoke_result
        logging.debug(msg)

        for subsock in subSocks:
            try:
                subsock.send_json(msg, zmq.DONTWAIT)
                logging.debug('发送成功!')
            except zmq.error.Again:
                logging.warning('暂无接收端.')

def doStart(endpoint):
    logging.info("Subscribed to {}".format(endpoint))
    global context
    global subSocks
    global stopped
    global upload_config
    global data_queue
    pull = context.socket(zmq.PULL)
    pull.setsockopt(zmq.RCVTIMEO, 1000)
    pull.connect(endpoint)
    # stopped = False
    while not stopped:
        try:
            data = pull.recv()
            logging.debug("Received some data, length: "+str(len(data))+", ready to upload..................................")
        except zmq.error.Again:
            continue

        if stopped:
            logging.debug("Received stop signal, stopping.................")
            return


        buf = QtCore.QByteArray.fromRawData(data)
        ds = QtCore.QDataStream(buf)
        msg_context = json.loads(ds.readQString())

        len_data = ds.readRawData(4)
        int_len_data = int.from_bytes(len_data, "big")
        # print(int_len_data)
        data = ds.readRawData(int_len_data)
        # files = {'file': ('slice.jpg', data)}

        data_queue.append((data,msg_context))


    pull.close()
    logging.debug("Stopped pull from {}".format(endpoint))


def start(command):
    global subscriberThreads
    global stopped
    if not stopped:
        stopped = True
        time.sleep(1)
        stopped = False
    else:
        stopped = False

    sendThread = threading.Thread(target=doSend)
    sendThread.start()
    if 'endpoints' in command:
        endpoints = command['endpoints']
        for endpoint in endpoints:
            consumeThread = threading.Thread(target = doStart,args = (endpoint,))
            consumeThread.start()
    if 'endpoint' in command:
        endpoint = command['endpoint']
        consumeThread = threading.Thread(target=doStart, args=(endpoint,))
        consumeThread.start()
    return 0

def config(command):
    global upload_config
    global context
    upload_config['url'] = command['url']
    logging.info("Config succeed!")
    return 0


def stop(command):
    logging.debug('Stopping......................')
    global stopped
    stopped = True
    return 0


def handleCommand( clientSocket):
    while True:
        try:
            data = clientSocket.recv(1024)
            # logging.debug("Received some message.")
            if not data:
                # logging.error("收到异常指令！")
                break
            cmd_str = data.decode('UTF-8')
            if len(cmd_str) == 0:
                # logging.error("收到异常指令！")
                break
            logging.debug("Received command: {}".format(cmd_str))
            command = json.loads(cmd_str)
            if command['command'] == 'start':
                result = start(command)
                # continue
            if command['command'] == 'config':
                result = config(command)
                # continue
            if command['command'] == 'subscribe':
                result = subscribe(command)
                # continue
            if command['command'] == 'stop':
                result = stop(command)
                # continue
            clientSocket.send(result.to_bytes(4,'big'))
        except ConnectionResetError as ce:
            return
        except Exception as e:
            traceback.print_exc()
            logging.error(e)
            clientSocket.close()
            return
    clientSocket.close()

if __name__ == '__main__':
    socket = socket.socket()
    socket.bind(("", 8080))
    socket.listen()
    logging.info("uploader started......")
    while True:
        clientSocket,clientAddress = socket.accept()
        handleCommandThread = threading.Thread(target=handleCommand, args=(clientSocket,))
        handleCommandThread.start()

