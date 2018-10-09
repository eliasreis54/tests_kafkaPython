from pykafka.common import OffsetType
from pykafka import KafkaClient
import time

msg_count = 100000
msg_size = 100

def calculate_thoughput(timing, n_messages, msg_size):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))

def pykafka_consumer_performance(use_rdkafka= False):
    # Setup client
    client = KafkaClient(hosts="172.23.0.8:9092")
    topic = client.topics[b'pykafka-test-topic']

    msg_consumed_count = 0
    
    consumer_start = time.time()
    # Consumer starts polling messages in background thread, need to start timer here
    consumer = topic.get_simple_consumer(use_rdkafka=use_rdkafka,
        auto_offset_reset=OffsetType.LATEST,
        reset_offset_on_start=True)
    
    for msg in consumer:
        if (not msg_consumed_count):
            consumer_start = time.time()
            msg_consumed_count += 1
        if msg_consumed_count >= msg_count:
            break
                        
    consumer_timing = time.time() - consumer_start
    consumer.stop()    
    calculate_thoughput(consumer_timing, msg_count, msg_size)

pykafka_consumer_performance()    