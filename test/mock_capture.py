import json, sys, time, zmq
import logging

from PyQt5.QtCore import QDataStream, QByteArray, QIODevice

index=0

def send(sock):

    global index
    data = QByteArray()
    stream = QDataStream(data, QIODevice.WriteOnly)
    body = dict()
    body['barcode'] = 'WD000204543C1053'
    body['id'] = "WD000204543C"
    imagefile = "slice.jpg"
    body['filename'] = imagefile
    print('Send {}'.format(body))
    stream.writeQString(json.dumps(body))

    with open(imagefile, "rb") as img_file:
        bytes = img_file.read()
        stream.writeBytes(bytes)

    try:
        sock.send(data, zmq.DONTWAIT)
        print('Send image succeed!')
    except:
        pass


if __name__ == '__main__':
    PORT=5555
    context = zmq.Context(2)
    push_socket = context.socket(zmq.PUSH)
    push_socket.set_hwm(5)
    push_socket.bind("tcp://*:{}".format(PORT))
    while True:
        send(push_socket)
        time.sleep(1)