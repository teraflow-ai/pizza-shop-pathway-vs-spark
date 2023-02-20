from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TOPIC = "orders"
BOOTSTRAP_SERVER = "localhost:29092"


def read_from_kafka(spark: SparkSession) -> DataFrame:
    options = {
        "startingOffsets": "earliest",
        "subscribe": TOPIC,
        "kafka.bootstrap.servers": BOOTSTRAP_SERVER,
    }

    data = spark.readStream.format("kafka").options(**options).load()
    return data


def process_orders_stream(orders_stream: DataFrame) -> DataFrame:
    schema = StructType(
        [
            StructField("createdAt", TimestampType(), False),
            StructField("id", StringType(), False),
            StructField("price", FloatType(), False),
            StructField("userId", IntegerType(), False),
            StructField(
                "items",
                ArrayType(
                    (
                        StructType(
                            [
                                StructField("productId", StringType(), False),
                                StructField("quantity", IntegerType(), False),
                                StructField("price", FloatType(), False),
                            ]
                        )
                    )
                ),
                False,
            ),
        ]
    )

    df_orders = (
        orders_stream.selectExpr("CAST(value AS STRING)")
        .select(F.from_json("value", schema=schema).alias("data"))
        .select(
            "data.createdAt",
            "data.id",
            "data.price",
            "data.userId",
            "data.items",
        )
    )

    df_orders_process = df_orders.select(
        "*", (F.explode("items")).alias("items_unnest")
    ).select(
        F.col("createdAt").alias("ts"),
        F.col("id").alias("order_id"),
        F.col("userId").alias("user_id"),
        F.col("price").alias("order_price"),
        F.col("items_unnest.productId").alias("product_id"),
        F.col("items_unnest.price").alias("product_price"),
        F.col("items_unnest.quantity"),
    )

    return df_orders_process


def get_num_pizzas_sold_by_type(enriched_orders_df: DataFrame) -> DataFrame:
    num_pizza_sold = enriched_orders_df.groupBy("product_id").agg(
        F.sum("quantity").alias("num_sold")
    )

    return num_pizza_sold


def main():
    spark = SparkSession.builder.appName(
        "Spark Pizza Empire - Number Pizzas Sold"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    orders_stream = read_from_kafka(spark)

    processed_orders_df = process_orders_stream(orders_stream)

    num_pizzas_sold_by_type = get_num_pizzas_sold_by_type(processed_orders_df)

    num_pizzas_sold_by_type_query = (
        num_pizzas_sold_by_type.coalesce(1)
        .writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    num_pizzas_sold_by_type_query.awaitTermination()


if __name__ == "__main__":
    main()
