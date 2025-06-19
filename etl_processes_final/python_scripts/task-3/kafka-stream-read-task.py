from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType, FloatType


def write_batch(batch, id):
    batch.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://rc1a-4lv719lifmpcj20q.mdb.yandexcloud.net:8443/etl-final") \
        .option("dbtable", "processed_kafka_comments") \
        .option("user", "db_user") \
        .option("password", "p4ssw0rd") \
        .option("driver", "org.clickhouse.Driver") \
        .mode("append") \
        .save()


def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-read-stream") \
        .getOrCreate()

    schema = StructType() \
        .add("comment_id", StringType()) \
        .add("score", FloatType()) \
        .add("self_text", StringType()) \
        .add("subreddit", StringType()) \
        .add("created_time", StringType()) \
        .add("post_id", StringType()) \
        .add("author_name", StringType()) \
        .add("controversiality", FloatType()) \
        .add("ups", IntegerType()) \
        .add("downs", IntegerType()) \
        .add("user_is_verified", BooleanType()) \
        .add("user_account_created_time", StringType()) \
        .add("user_awardee_karma", FloatType()) \
        .add("user_awarder_karma", FloatType()) \
        .add("user_link_karma", FloatType()) \
        .add("user_comment_karma", FloatType()) \
        .add("user_total_karma", FloatType()) \
        .add("post_score", IntegerType()) \
        .add("post_self_text", StringType()) \
        .add("post_title", StringType()) \
        .add("post_upvote_ratio", FloatType()) \
        .add("post_thumbs_ups", IntegerType()) \
        .add("post_total_awards_received", IntegerType()) \
        .add("post_created_time", StringType())

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
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("created_time", to_timestamp(col("created_time").cast("string"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("user_account_created_time",
                    to_timestamp(col("user_account_created_time").cast("string"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("post_created_time",
                    to_timestamp(col("post_created_time").cast("string"), "yyyy-MM-dd HH:mm:ss"))

    query = df.writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", "s3a://dataproc-main-bucket/kafka-postgres-checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()


if __name__ == 'main':
    main()
