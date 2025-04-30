from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer

schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

schema_str = open("/app/../producer/student.avsc", "r").read()
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'student-group',
    'auto.offset.reset': 'earliest',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['students'])

print("Waiting for messages...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    student = msg.value()
    if student:
        print(f"Received student record: {student}")
