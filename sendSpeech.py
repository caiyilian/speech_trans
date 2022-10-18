import math
import random
from paho.mqtt import client as mqtt_client
import os
import time
import threading
import pyaudio
import wave

os.chdir("temp")
broker = 'test.ranye-iot.net'
port = 1883
topic = "huaguang/"
client_id = f'python-mqtt-{random.randint(0, 1000)}'
writeFileNum = 0


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


def publish(client, sendList):
    client.publish(topic + "start", "")
    for index, content in enumerate(sendList):
        client.publish(topic + str(index), content, qos=0)
        time.sleep(0.01)
    client.publish(topic + "end", "")


class RecordThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.bRecord = True

    def openFile(self, count):
        wavfile = wave.open(str(count) + ".wav", 'wb')
        wavfile.setnchannels(1)
        wavfile.setsampwidth(2)
        wavfile.setframerate(16000)
        return wavfile

    def run(self):
        audio = pyaudio.PyAudio()
        wavstream = audio.open(format=pyaudio.paInt16,
                               channels=1,
                               rate=16000,
                               input=True,
                               frames_per_buffer=1024)
        global writeFileNum
        while self.bRecord:
            start = time.time()
            wavFile = self.openFile(writeFileNum)
            while time.time() - start < 3:
                wavFile.writeframes(wavstream.read(1024))
            wavFile.close()
            writeFileNum += 1
        wavstream.stop_stream()
        wavstream.close()
        audio.terminate()

    def stoprecord(self):
        self.bRecord = False


def sendAudio(client, readFileNum):
    mp3File = open(f"{readFileNum}.mp3", 'rb')
    data = mp3File.read()
    mp3File.close()
    sendList = []
    batchsNum = math.ceil(len(data) / 100)
    for i in range(batchsNum):
        try:
            sendList.append(data[i * 100:(i + 1) * 100])
        except:
            sendList.append(data[i * 100:])

    publish(client, sendList)


audio_record = RecordThread()
audio_record.start()
print("开始")
readFileNum = 0
client = connect_mqtt()
while True:
    while writeFileNum == readFileNum:
        pass
    os.system(f"ffmpeg -i {readFileNum}.wav -b:a 8k -acodec mp3 -y {readFileNum}.mp3&del {readFileNum-1}.wav {readFileNum-1}.mp3")
    sendAudio(client, readFileNum)

    readFileNum += 1
audio_record.stoprecord()
