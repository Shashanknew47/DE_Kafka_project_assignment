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


import argparse
# A simple example demonstrating use of JSONSerializer.
import os
from typing import List
from uuid import uuid4

#from confluent_kafka.schema_registry import *
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import (MessageField, SerializationContext,
                                           StringSerializer)
from dotenv import load_dotenv
from six.moves import input

load_dotenv()

FILE_PATH = "/Users/shashankjain/Desktop/Practice/Ineuron/Kafka/DE_Kafka_project_assignment/Assignment-1/restaurant_orders.csv"



def sasl_conf():

    sasl_conf = {'sasl.mechanism': os.environ['SSL_MACHENISM'],
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':os.environ['BOOTSTRAP_SERVER'],
                'security.protocol': os.environ['SECURITY_PROTOCOL'],
                'sasl.username': os.environ['API_KEY'],
                'sasl.password': os.environ['API_SECRET_KEY']
                }
    return sasl_conf



def schema_config():
    return {'url':os.environ['ENDPOINT_SCHEMA_URL'],

    'basic.auth.user.info':f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:{os.environ['SCHEMA_REGISTRY_API_SECRET']}"

    }
