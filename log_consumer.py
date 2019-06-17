from kafka import KafkaConsumer
from log_parser import LogRow
from pyspark import SparkContext, SparkConf
from code.blackLister import BlackLister
import time

# sc = SparkContext(appName="Demo App")
# sc.setLogLevel("WARN")
# ssc = StreamingContext(sc, 6)
#
# kafkaStream = KafkaUtils.createStream(ssc,, group_id, {'test': 1})



if __name__ == "__main__":

    conf = SparkConf().setAppName("demo")
    sc = SparkContext(conf=conf)

    kafka_topic = 'test'
    group_id = 'log_consumer_group'

    #create Kafka consumer

    consumer = KafkaConsumer(kafka_topic, auto_offset_reset='earliest')
    lr = LogRow()

    #loop through
    while True:
        # for msg in consumer:
        #        msg = msg.value.decode('ascii')
        #        msg = lr.parseRow(msg)
        #        messages.append(msg)
        #        #pp = pprint.PrettyPrinter(indent=4)
        #        #pp.pprint(msg)
        window = consumer.poll(timeout_ms=6000)
        #sc.parallelize()
        queue = []
        for tp, messages in window.items():
            for message in messages:
                queue.append(message.value.decode('ascii'))
        queue_rdd = sc.parallelize(queue)
        bl = BlackLister(queue_rdd)
        bl.write2file('lists/blacklist.txt')
        time.sleep(5)


#parse message
# 200.4.91.190 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"
