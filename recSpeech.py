import base64
from paho.mqtt import client as mqtt_client
import random
import wave
import pyaudio
import threading
import time
import playsound
import os

os.chdir("temp2")
readFileNum = 0
broker = 'test.ranye-iot.net'
port = 1883
topic = "huaguang/#"
client_id = f'python-mqtt-{random.randint(0, 1000)}'


class myThread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self, fileName):
        threading.Thread.__init__(self)
        self.fileName = fileName
        pass

    def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
        playsound.playsound(self.fileName)


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        global recList, readFileNum
        if msg.topic == "huaguang/start":
            recList = []
            start = time.time()
        elif msg.topic == "huaguang/end":
            total = b"".join(recList)
            f = open(f"{readFileNum}.mp3", "wb")
            f.write(total)
            f.close()
            myThread(f"{readFileNum}.mp3").start()
            readFileNum += 1
        else:
            recList.append(msg.payload)

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
