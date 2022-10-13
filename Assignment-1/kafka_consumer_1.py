import client
import file_object
import schema
import csv
import os


from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer




def append_output(record):
    cpath = os.getcwd()
    exten = 'Assignment-1'
    path = os.path.join(cpath,exten)

    val = os.path.isfile(os.path.join(path,'output-1.csv'))

    with open('Assignment-1/output-1.csv','a') as file:
        headers = ['Order Number','Order Date','Item Name','Quantity','Product Price','Total Products']

        csv_writer = csv.DictWriter(file, fieldnames=headers)

        if not val:
            csv_writer.writeheader()

        csv_writer.writerow(record)


def main(topic):

    schema_str = schema.create_schema_str_latest('restaurent-take-away-data-value')


    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=file_object.Restaurant.dict_to_restaurant)

    consumer_conf = client.sasl_conf()
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

                append_output(restaurant_record.record)



        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")