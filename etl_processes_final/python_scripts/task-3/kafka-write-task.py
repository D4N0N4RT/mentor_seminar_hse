from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, struct


def send_batch_to_kafka(iterator):
    for row in iterator:
        row.write.format('kafka') \
            .option('kafka.bootstrap.servers', 'rc1a-bmibv8b7h7v6d78n.mdb.yandexcloud.net:9091') \
            .option('topic', 'dataproc-topic') \
            .option('kafka.security.protocol', 'SASL_SSL') \
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
            .option('kafka.sasl.jaas.config',
                    "org.apache.kafka.common.security.scram.ScramLoginModule required "
                    "username=\"user1\" "
                    "password=\"password1\";") \
            .save()
    yield "Partition Processed"


def main():
    spark = SparkSession.builder \
        .appName("parquet-to-kafka-spark") \
        .getOrCreate()

    df = spark.read.parquet("s3a://data-proc-main-bucket/processed-data/reddit_opinion_climate_change.parquet").cache()

    df = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))
    df.rdd.mapPartitions(send_batch_to_kafka).collect()


if __name__ == 'main':
    main()
