from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, col, struct


def main():
    spark = SparkSession.builder.appName('etl-kafka-write-app').getOrCreate()

    df = spark.createDataFrame([
        Row(msg='Message #1 from dataproc'),
        Row(msg='Message #2 from dataproc')
    ])

    df = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))
    df.write.format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1a-bmibv8b7h7v6d78n.mdb.yandexcloud.net:9091') \
        .option('topic', 'etl-processes-topic') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.sasl.jaas.config',
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password = password1 "
                ";") \
        .save()


if __name__ == 'main':
    main()
