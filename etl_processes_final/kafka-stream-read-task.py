from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType


def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-read-stream") \
        .getOrCreate()

    kafka_df = spark.readStream.format("kafka") \
        .option('kafka.bootstrap.servers', 'rc1a-bmibv8b7h7v6d78n.mdb.yandexcloud.net:9091') \
        .option("subscribe", "dataproc-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=\"user1\" "
                "password=\"password1\";") \
        .option("startingOffsets", "latest") \
        .load()

    df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), ).alias("data")) \
        .select("data.*")


if __name__ == 'main':
    main()
