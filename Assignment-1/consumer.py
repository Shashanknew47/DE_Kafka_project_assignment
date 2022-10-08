import client
import file_object
import schema
import csv


from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer



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

                file_object.append_output(restaurant_record.record)



        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")