from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
import json

schema_str = open("student.avsc", "r").read()

schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'key.serializer': str.encode,
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

topic = "students"

while True:
    name = input("Enter student name: ")
    age = int(input("Enter student age: "))
    grade = input("Enter grade: ")
    
    student = {"name": name, "age": age, "grade": grade}
    producer.produce(topic=topic, key=name, value=student)
    producer.flush()
    print("Sent:", student)
