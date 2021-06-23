from kafka import KafkaConsumer
from json import loads, dump


kafka_topic_name = "test-123"
bootstrap_server = "192.168.1.14:9092"
group_id = "consumer-group-a"

if __name__=="__main__":
    consumer = KafkaConsumer(kafka_topic_name, bootstrap_servers=bootstrap_server,
                             auto_offset_reset='earliest', group_id = group_id)
    
    for message in consumer:
        with open('file_test.json','a')as file:
            #file.write(str(loads(message.value)))
            dump(str(loads(message.value)),file)
        print("Registered User = {}".format(loads(message.value)))
