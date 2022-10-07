from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


from dotenv import load_dotenv
import os


load_dotenv()


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


class Restaurant:
    def __init__(self,record) -> None:
        for k,v in record.items():
            self.k = v

        self.record = record

    def dict_to_restaurant(self,data,ctx):
        return Restaurant(record=data)

    def __str__(self) -> str:
        return f'Restaurant({self.record})'




def main(topic):


    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    subject = schema_registry_client.get_subjects()
    schema_str = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str


    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Restaurant.dict_to_car)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurant_record = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if restaurant_record is not None:
                print("User record {}: restaurant_record: {}\n"
                      .format(msg.key(), restaurant_record))
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")