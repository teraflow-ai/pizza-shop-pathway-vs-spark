import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (ArrayType, FloatType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

TOPIC_ORDERS = "orders"
TOPIC_PRODUCTS = "products"
BOOTSTRAP_SERVER = "localhost:29092"


def read_orders_from_kafka(spark: SparkSession) -> DataFrame:
    options = {
        "startingOffsets": "earliest",
        "subscribe": TOPIC_ORDERS,
        "kafka.bootstrap.servers": BOOTSTRAP_SERVER,
    }

    data = spark.readStream.format("kafka").options(**options).load()
    return data


def read_products_from_kafka(spark: SparkSession) -> DataFrame:
    options = {
        "subscribe": TOPIC_PRODUCTS,
        "kafka.bootstrap.servers": BOOTSTRAP_SERVER,
    }

    data = spark.readStream.format("kafka").options(**options).load()
    return data


def enrich_orders_stream(orders_stream: DataFrame) -> DataFrame:
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

    df_orders_flattened = df_orders.select(
        "*", (F.explode("items")).alias("items_unnest")
    ).select(
        F.col("createdAt").alias("ts"),
        F.col("id").alias("order_id"),
        F.col("userId").alias("user_id"),
        F.col("price").alias("order_price"),
        F.col("items_unnest.productId").cast(IntegerType()).alias("product_id"),
        F.col("items_unnest.price").alias("product_price"),
        F.col("items_unnest.quantity"),
    )

    return df_orders_flattened


def enrich_products_stream(products_stream: DataFrame) -> DataFrame:
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("id", StringType(), False),
            StructField("description", StringType(), False),
            StructField("category", StringType(), False),
            StructField("image", StringType(), False),
            StructField("price", FloatType(), False),
        ]
    )

    df_products = (
        products_stream.selectExpr("CAST(value AS STRING)")
        .select(F.from_json("value", schema=schema).alias("data"))
        .select(
            F.col("data.id").cast(IntegerType()).alias("product_id"),
            "data.name",
            "data.description",
            "data.category",
            "data.price",
            "data.image",
        )
    )

    return df_products


def main():
    spark = SparkSession.builder.appName(
        "Spark Pizza Empire - Join Product and Orders"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    products_stream = read_products_from_kafka(spark)
    products_df = enrich_products_stream(products_stream)

    orders_stream = read_orders_from_kafka(spark)
    orders_df = enrich_orders_stream(orders_stream)

    enriched_orders_df = orders_df.join(products_df, on="product_id")

    enriched_orders_query = (
        enriched_orders_df.coalesce(1)
        .writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    enriched_orders_query.awaitTermination()


if __name__ == "__main__":
    main()
