from confluent_kafka import Consumer, KafkaError , KafkaException
import sys
import random
import json
import requests
me = 'fatma'
groupId=me+'-group1'

conf = {'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
         'group.id':groupId,
         'enable.auto.commit':True,
          'auto.offset.reset':'smallest'
}

my_Consumer =Consumer(conf)
topics = [me]
my_Consumer.subscribe(topics)

def new_msg(msg):
  choice = random.choice(['car' ,'tree' , 'human', 'house'])
  id=msg.value().decode('utf-8')
  res=requests.put('http://127.0.0.1:5000/object/' + id , json={'object':choice})
  print(f"Response from server: {res.status_code}, {res.text}")

try:
   while True:
            msg = my_Consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                new_msg(msg)
finally:
        # Close down consumer to commit final offsets.
        my_Consumer.close()

