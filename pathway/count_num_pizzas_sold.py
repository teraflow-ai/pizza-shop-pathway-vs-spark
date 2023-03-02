import pathway as pw
from datetime import datetime
import json
from pathway.stdlib.utils.col import unpack_col
from pathway.stdlib.utils.col import flatten_column


TOPIC = "orders"
BOOTSTRAP_SERVER = "localhost:29092"
RDKAFKA_SETTINGS = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "localhost",
}


def read_from_kafka():
    raw_data = pw.kafka.read(
        RDKAFKA_SETTINGS,
        topic_names=[TOPIC],
        format="raw",
        autocommit_duration_ms=2000,
    )

    return raw_data


def process_orders_data(orders_data: str):
    data_parsed = json.loads(orders_data)
    created_at = datetime.strptime(data_parsed["createdAt"], "%Y-%m-%dT%H:%M:%S.%f")
    created_at = str(data_parsed["createdAt"])
    id_ = str(data_parsed["id"])
    price = float(data_parsed["price"])
    user_id = int(data_parsed["userId"])
    items = tuple(str(item) for item in data_parsed["items"])

    return (created_at, id_, price, user_id, items)


def main():
    orders_stream = read_from_kafka()
    processed_orders_raw = orders_stream.select(
        processed=pw.apply(process_orders_raw, pw.this.data)
    )
    processed_orders_raw = unpack_col(
        processed_orders_raw.processed, "createdAt", "id_", "price", "userId", "items"
    )
    processed_orders_raw.join(flatten_column(processed_orders_df.items))
)
    processed_orders_df  = flatten_column(processed_orders_df.items)

    pw.csv.write(processed_orders_df, filename="test.csv")
    pw.run()


if __name__ == "__main__":
    main()
