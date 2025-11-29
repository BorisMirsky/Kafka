import kafka-python 
import json
import random
import time

from kafka import KafkaProducer
# Connect to Kafka server
producer = KafkaProducer(
  bootstrap_servers=['URL-адрес_вашего_инстанса_Kafka:порт_обычно_9092'],
  sasl_mechanism='SCRAM-SHA-256',
  security_protocol='SASL_SSL',
  sasl_plain_username='имя_вашего пользователя',
  sasl_plain_password='пароль_вашего_пользователя',
  value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Prepare a list of devices
devices =[random.randint(0, 100) for i in range(100)]

# Prepare a list of possible routing keys
measures = ['pressure', 'temperature']

while True:
    # Prepare random message in JSON format
    measure=random.choice(measures)
    data = {'device': random.choice(devices), 'measure': measure, 'value': random.randint(0,100)}
    message = json.dumps(data)
    if measure=='pressure':
     partition_key = 1
    else:
     partition_key = 2

    print(f' [x] partition {partition_key}')  
    print(f' [x] Message {message}')
    
    # produce json messages
    future = producer.send('anna_demo_topic', value=data, partition=partition_key)

    record_metadata = future.get(timeout=60)
    print(f' [x] Sent {record_metadata}')

    # Sleep for 3 seconds
    time.sleep(3)

# close producer
producer.close()
