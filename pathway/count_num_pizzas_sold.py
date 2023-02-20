import pathway as pw
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


def enrich_orders_stream(orders_stream):
    # t = orders_stream.select(
    #     createdAt=pw.this.data.createdAt,
    # )
    # value_columns=["createdAt", "id", "price", "userId", "items"],
    t = orders_stream
    return t


def main():
    orders_stream = read_from_kafka()
    enriched_orders_df = enrich_orders_stream(orders_stream)

    # t = raw_data.select(price=pw.apply_with_type(float, float, raw_data.price))
    # t = t.reduce(sum=pw.reducers.sum(t.price))
    # pw.kafka.write(
    #     enriched_orders_df, RDKAFKA_SETTINGS, topic_name="orders-total", format="json"
    # )
    pw.csv.write(enriched_orders_df, filename="test.csv")
    pw.run()


if __name__ == "__main__":
    main()
