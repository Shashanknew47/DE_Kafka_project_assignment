import client

def create_schema_str_latest(subject):
    return client.schema_registry_client.get_latest_version(subject).schema.schema_str


def get_subjects():
    return client.schema_registry_client.get_subjects()

schema_str_latest = create_schema_str_latest('restaurent-take-away-data-value')


if __name__ == "__main__":
    print(schema_str_latest)
    print(get_subjects())