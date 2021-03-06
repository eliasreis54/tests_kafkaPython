from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
import json
import time

msg_count = 100000
msg_size = 100
msg_payload = ('kafkatest' * 20).encode()[:msg_size]

kf_prod = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                bootstrap_servers="172.23.0.8:9092")

def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))


def send_notification(event):
    if kf_prod is None:
        print('Tried to send a notification when there is no broker yet. Ignoring.')
        return

    try:
        topic = 'pykafka-test-topic'
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
            start = time.time()
            x = 1
            while x <= msg_count:
                send_notification(msg_payload)
                x += 1
            finish = time.time()
            calculate_thoughput((finish - start), msg_count, msg_size)
            time.sleep(5)
            # execute the command
        except Exception, e:
            print e
        y = y+1

testVolumeMessages()