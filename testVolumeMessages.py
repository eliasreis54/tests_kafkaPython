from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
import json
import datetime
import time

kf_prod = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                bootstrap_servers='172.23.0.14:9092')


def send_notification(event):
    if kf_prod is None:
        print('Tried to send a notification when there is no broker yet. Ignoring.')
        return

    try:
        topic = '7db62042-c7de-45ef-b8c2-29e001067bdd'
        kf_prod.send(topic, event)
        kf_prod.flush()
    except KafkaTimeoutError:
        print("Kafka timed out.")

def testVolumeMessages():
    y = 1
    while y <= 5:
        print('-'*100)
        print('test number: {}'.format(y))
        try:
            start = datetime.datetime.now()
            print('start: {}'.format(start))
            x = 1
            while x <= 3000:
                send_notification({"type": "performance", 
                "msg_number": x,
                "time": time.time()})
                x += 1
            finish = datetime.datetime.now()
            print('finish: {}'.format(finish))
            print('Total: {}'.format(finish - start))
            time.sleep(5)
            # execute the command
        except Exception, e:
            print e
        y = y+1

testVolumeMessages()