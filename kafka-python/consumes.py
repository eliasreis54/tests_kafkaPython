from kafka import KafkaConsumer
import time
msg_count = 100000
msg_size = 100

def calculate_thoughput(timing, n_messages, msg_size):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))
    
def python_kafka_consumer_performance():
    topic = 'pykafka-test-topic'
    consumer = KafkaConsumer(
        bootstrap_servers="172.23.0.8:9092",
        auto_offset_reset = 'latest', # start at earliest topic
        group_id = None # do no offest commit
    )
    msg_consumed_count = 0
            
    consumer.subscribe([topic])
    for msg in consumer:
        if (not msg_consumed_count):
            consumer_start = time.time()
            msg_consumed_count += 1
        if msg_consumed_count >= msg_count:
            break
                    
    consumer_timing = time.time() - consumer_start
    consumer.close()   
    calculate_thoughput(consumer_timing, msg_consumed_count, msg_size)
python_kafka_consumer_performance()