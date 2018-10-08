from kafka import KafkaConsumer
import time

def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))
    
def python_kafka_consumer_performance():
    topic = '7db62042-c7de-45ef-b8c2-29e001067bdd'
    msg_count = 10000
    consumer = KafkaConsumer(
        bootstrap_servers='172.23.0.14:9092',
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
    calculate_thoughput(consumer_timing, msg_consumed_count, msg_count)
python_kafka_consumer_performance()