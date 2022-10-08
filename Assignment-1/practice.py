import client
import file_object
import schema


from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import (MessageField, SerializationContext,
                                           StringSerializer)

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

    schema_str = schema.create_schema_str_latest('restaurent-take-away-data-value')
    schema_registry_client = SchemaRegistryClient(client.schema_config())


    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, restaurant_to_dict)


    producer = Producer(client.sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()


    producer.poll(0.0)

    try:
        FILE_PATH = "/Users/shashankjain/Desktop/Practice/Ineuron/Kafka/DE_Kafka_project_assignment/Assignment-1/restaurant_orders.csv"
        for restaurant in file_object.get_restaurant_instance(filepath=FILE_PATH):

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
