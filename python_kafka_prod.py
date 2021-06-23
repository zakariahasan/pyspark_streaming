from kafka import KafkaProducer
from faker import Faker
from json import dumps


kafka_topic_name = "test-123"
bootsprap_server = "192.168.1.14:9092"

producer = KafkaProducer(bootstrap_servers=bootsprap_server,
                         value_serializer= lambda x: dumps(x).encode("utf-8"))
    


if __name__ == "__main__":
    
    while True:
        fake = Faker()
        message ={"name":fake.name(),"address":fake.address()}
        message
    
        producer.send(kafka_topic_name,message)
      
