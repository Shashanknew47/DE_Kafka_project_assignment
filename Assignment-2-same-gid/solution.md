1. Setup Confluent Kafka Account

## 2. Create one kafka topic named as "restaurent-take-away-data" with 3 partitions


## 3. Setup key (string) & value (json) schema in the confluent schema registry


## 4. Write a kafka producer program (python or any other language) to read data records from restaurent data csv file,make sure schema is not hardcoded in the producer code, read the latest version of schema and schema_str from schema registry and use it for data serialization.

client.py
``` python
import os
from dotenv import load_dotenv
from confluent_kafka.schema_registry import SchemaRegistryClient



def schema_config():
    return {'url':os.environ["ENDPOINT_SCHEMA_URL"],

    'basic.auth.user.info':f"""{os.environ["SCHEMA_REGISTRY_API_KEY"]}:{os.environ["SCHEMA_REGISTRY_API_SECRET"]}"""

    }

schema_registry_conf = schema_config()
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

```


schema.py
``` python
import client

def create_schema_str_latest(subject):
    return client.schema_registry_client.get_latest_version(subject).schema.schema_str

```


## 5. From producer code, publish data in Kafka Topic one by one and use dynamic key while publishing the records into the Kafka Topic


``` python

try:
    FILE_PATH = "/Users/shashankjain/Desktop/Practice/Ineuron/Kafka/DE_Kafka_project_assignment/Assignment-1/restaurant_orders.csv"
    i = 0
    for restaurant in file_object.get_restaurant_instance(filepath=FILE_PATH):
        i += 1
        print(i)
        print(restaurant)
        producer.produce(topic=topic,
                        key=string_serializer(str(uuid4()), restaurant_to_dict),
                        value=json_serializer(restaurant, SerializationContext(topic, MessageField.VALUE)),
                        on_delivery=delivery_report)

```


## 6. Write kafka consumer code and create two copies of same consumer code and save it with different names (kafka_consumer_1.py & kafka_consumer_2.py), again make sure lates schema version and schema_str is not hardcoded in the consumer code, read it automatically from the schema registry to desrialize the data.Now test two scenarios with your consumer code:

###    a.) Use "group.id" property in consumer config for both consumers and mention different group_ids in kafka_consumer_1.py & kafka_consumer_2.py,apply "earliest" offset property in both consumers and run these two consumers from two different terminals. Calculate how many records each consumer consumed and printed on the terminal

```
  Records printed by each each consumer :74,819
```






### b.) Use "group.id" property in consumer config for both consumers and mention same group_ids in kafka_consumer_1.py & kafka_consumer_2.py, apply "earliest" offset property in both consumers and run these two consumers from two different terminals. Calculate how many records each consumer consumed and printed on the terminal



## 7. Once above questions are done, write another kafka consumer to read data from kafka topic and from the consumer code create one csv file "output.csv" and append consumed records output.csv file.
