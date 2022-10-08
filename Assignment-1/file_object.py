import csv


class Restaurant:
    def __init__(self,record) -> None:
        for k,v in record.items():
            self.k = v

        self.record = record

    @staticmethod
    def dict_to_restaurant(data,ctx):
        return Restaurant(record=data)

    def __str__(self) -> str:
        return f'Restaurant({self.record})'



# Converting string values into correct type. For schema validation
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


# Generating Restaurant object for each record of file
def get_restaurant_instance(filepath):
    with open(filepath,'r') as file:
        f = csv.DictReader(file)
        for i in f:
            i = cast_str_int(i)
            yield (Restaurant(i))


def append_output(record):
    with open('Assignment-1/output.csv','a') as file:
        headers = ['Order Number','Order Date','Item Name','Quantity','Product Price','Total Products']

        csv_writer = csv.DictWriter(file, fieldnames=headers)
        csv_writer.writeheader()

        csv_writer.writerow(record)



if __name__ == '__main__':
    FILE_PATH = "/Users/shashankjain/Desktop/Practice/Ineuron/Kafka/DE_Kafka_project_assignment/Assignment-1/restaurant_orders.csv"
    r = get_restaurant_instance(FILE_PATH)
    print(next(r))
    print(next(r))
    record  = {'Order Number': '16118', 'Order Date': '03/08/2019 20:25', 'Item Name': 'Plain Papadum', 'Quantity': '2', 'Product Price': '0.8', 'Total Products': '6'}

    append_output(record)

    
