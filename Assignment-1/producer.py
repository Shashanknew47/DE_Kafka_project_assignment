#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



# A simple example demonstrating use of JSONSerializer.
import os
from uuid import uuid4
import csv


from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import (MessageField, SerializationContext,
                                           StringSerializer)
from dotenv import load_dotenv


load_dotenv()

FILE_PATH = "/Users/shashankjain/Desktop/Practice/Ineuron/Kafka/DE_Kafka_project_assignment/Assignment-1/restaurant_orders.csv"


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



def cast_str_int(d):
    new_di = {}
    for k,v in d.items():
        if k in ['Quantity','Total Products']:
            new_di[k] = int(v)
        elif k in ['Product Price']:
            new_di[k] = float(v)

        else:
            new_di[k] = v

    return new_di

def get_restaurant_instance(filepath):
    with open(filepath,'r') as file:
        f = csv.DictReader(file)
        for i in f:
            i = cast_str_int(i)
            yield (Restaurant(i))



# def get_car_instance(file_path):
#     df=pd.read_csv(file_path)
#     df=df.iloc[:,1:]
#     cars:List[Car]=[]
#     for data in df.values:
#         car=Car(dict(zip(columns,data)))
#         cars.append(car)
#         yield car



def restaurant_to_dict(restaurant, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return restaurant.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return None

    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))




def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    subject = schema_registry_client.get_subjects()
    schema_str = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str


    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, restaurant_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for restaurant in get_restaurant_instance(filepath=FILE_PATH):

            print(restaurant)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), restaurant_to_dict),
                            value=json_serializer(restaurant, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()


main("restaurent-take-away-data")
