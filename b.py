# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer

# Import sys module
import sys

# Define server with port
bootstrap_servers = ['192.168.56.1:9092']

# Define topic name from where the message will recieve
topicName = 'First_Topic'

from kafka import KafkaConsumer
import logging

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

consumidor= KafkaConsumer("MI_TOPIC",
                        bootstrap_servers= bootstrap_servers,
                        group_id=None,
                        auto_offset_reset='latest',
                        api_version=(0,10)
        )

#Pull messages every 2 seconds
consumidor.poll(timeout_ms=2000)

for msg in consumidor:
    mensaje = str(msg.value.decode('utf-8'))
    logging.info(f"====>>>>>: {str(msg)}")
# # Initialize consumer variable
# consumer = KafkaConsumer (topicName, group_id ='group1',bootstrap_servers =
#    bootstrap_servers)
# # Read and print message from consumer
# print(consumer)
# for msg in consumer:
#  print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))
#
# # Terminate the script
# sys.exit()