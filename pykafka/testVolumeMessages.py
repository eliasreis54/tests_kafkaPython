from pykafka import KafkaClient
import time
import datetime

msg_count = 100000
msg_size = 100
msg_payload = ('kafkatest' * 20).encode()[:msg_size]


    
def calculate_thoughput(timing, n_messages, msg_size):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))


def testVolumeMessages(use_rdkafka= True):

    # Setup client
    client = KafkaClient(hosts="172.23.0.8:9092")
    topic = client.topics[b'pykafka-test-topic']
    producer = topic.get_producer(use_rdkafka= use_rdkafka)
    y = 1
    while y <= 5:
        print('-'*100)
        print('test number: {}'.format(y))
        msgs_produced = 0 
        produce_start = time.time()
        for i in range(msg_count):
            # Start producing
            producer.produce(msg_payload)
            msgs_produced = msgs_produced+1
        calculate_thoughput((time.time() - produce_start), msgs_produced, msg_size)
        time.sleep(5)
        y = y+1
    producer.stop() # Will flush background queue
testVolumeMessages()