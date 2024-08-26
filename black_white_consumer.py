import io
import os
from PIL import Image
from confluent_kafka import Consumer, KafkaError , KafkaException
import sys



me = 'fatma'
groupId=me+'-group1'
topic = me
conf = {'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
         'group.id':groupId,
         'enable.auto.commit':True,
          'auto.offset.reset':'smallest'
}

Consumer =Consumer(conf)
topics = [me]
Consumer.subscribe(topics)



folder_path = '/home/fatma_ramadan/kafka_2.12-3.8.0/kafka_2.12-3.8.0/images'


for filename in os.listdir(folder_path):
    if filename.endswith(('.png', '.jpg', '.jpeg', '.bmp', '.tiff')):  
        img_path = os.path.join(folder_path, filename)

        
        with Image.open(img_path) as img:
            
            bw_img = img.convert('L')

           
            bw_img.save(os.path.join(folder_path,filename))

print("Congrats your images conveeted to black and white")

