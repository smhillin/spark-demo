#must start zookeeper
# brew services restart zookeeper
# brew services start kafka
# brew services list


from kafka import KafkaProducer
import time
partition=3

topic = 'test'
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=str.encode)

with open("apache-logs/logs", mode="r") as log:
    for line in log:
        producer.send(topic, value=line)




