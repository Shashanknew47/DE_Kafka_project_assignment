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


```
