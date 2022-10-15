import os
from dotenv import load_dotenv
from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka import Producer




load_dotenv()


# For connecting confluent Kafka Cluster
def sasl_conf():

    sasl_conf = {'sasl.mechanism': os.environ["SSL_MACHENISM"],
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':os.environ["BOOTSTRAP_SERVER"],
                'security.protocol': os.environ["SECURITY_PROTOCOL"],
                'sasl.username': os.environ["API_KEY"],
                'sasl.password': os.environ["API_SECRET_KEY"]
                }
    return sasl_conf



def schema_config():
    return {'url':os.environ["ENDPOINT_SCHEMA_URL"],

    'basic.auth.user.info':f"""{os.environ["SCHEMA_REGISTRY_API_KEY"]}:{os.environ["SCHEMA_REGISTRY_API_SECRET"]}"""

    }


schema_registry_conf = schema_config()
schema_registry_client = SchemaRegistryClient(schema_registry_conf)


# producer_client = Producer(sasl_conf())
