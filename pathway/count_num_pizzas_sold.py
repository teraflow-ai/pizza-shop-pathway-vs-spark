import pathway as pw


TOPIC = "orders"
BOOTSTRAP_SERVER = "localhost:29092"
RDKAFKA_SETTINGS = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "localhost",
    "session.timeout.ms": "6000",
}


def read_from_kafka():
    raw_data = pw.kafka.read(
        RDKAFKA_SETTINGS,
        topic_names=[TOPIC],
        format="json",
        value_columns=["createdAt", "id", "price", "userId"],
        autocommit_duration_ms=1000,
    )

    return raw_data


def main():
    raw_data = read_from_kafka()
    # t = raw_data.select(price=pw.apply_with_type(float, float, raw_data.price))
    # t = t.reduce(sum=pw.reducers.sum(t.price))
    pw.kafka.write(raw_data, RDKAFKA_SETTINGS, topic_name="orders-total", format="json")
    pw.run()


if __name__ == "__main__":
    main()
