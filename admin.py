from confluent_kafka.admin import NewTopic, AdminClient

# Create a KafkaAdminClient instance

conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
}

admin_client = AdminClient(conf)

# Define the topic configuration
me = 'fatma'
num_partitions = 3
replication_factor = 1

# Create a NewTopic object with the desired configuration
new_topic = NewTopic(me, num_partitions=num_partitions, replication_factor=replication_factor)

# Add the new topic to the Kafka admin client
admin_client.create_topics([new_topic])[me].result()



