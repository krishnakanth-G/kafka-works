from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
import pickle

consumer = KafkaConsumer('Ads',
			  bootstrap_servers=['localhost:9092'],
			  auto_offset_reset='earliest',
			  enable_auto_commit=True,
			  group_id='my-group',
			  value_deserializer = lambda x: loads(x.decode('utf-8')))
					     
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

def produce(incoming):

	loaded_model = pickle.load(open("model.sav", 'rb'))
	result = loaded_model.predict([incoming])
		
	data = {'UserAge, EstimatedSalary' : str(incoming),
			'Purchases Ads item': str(result)}
			
	producer.send('AdsResult', value=data)
	

for message in consumer:
	print('UserAge, EstimatedSalary' + str(message.value['UserAge, EstimatedSalary']))
	produce(message.value['UserAge, EstimatedSalary' ])

