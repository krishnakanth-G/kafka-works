from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

print('Streaming data is having age and estimated salary of users from social ads')

list_ = [[23,19000],[24,20000],[34,100000],[18,82000]]
                         
for d1 in list_:
    data = {'UserAge, EstimatedSalary' : d1}
    producer.send('Ads', value=data)
    sleep(1)
    
print('Streaming Done')

