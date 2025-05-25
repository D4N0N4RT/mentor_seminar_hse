from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.appName('etl-kafka-read-app').getOrCreate()

    query = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1a-bmibv8b7h7v6d78n.mdb.yandexcloud.net:9091') \
        .option('subscribe', 'etl-processes-topic') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.sasl.jaas.config',
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password = password1 "
                ";") \
        .option('startingOffsets', 'earliest') \
        .load() \
        .selectExpr('CAST(value AS String)') \
        .where(col('value').isNotNull) \
        .writeStream \
        .trigger(once=True) \
        .queryName('processed_messages') \
        .format('memory') \
        .start()

    query.awaitTermination()

    df = spark.sql('select value from processed_messages')

    df.write.format('text').save('s3a://data-proc-main-bucket/etl_hw13_kafka_output')


if __name__ == 'main':
    main()
