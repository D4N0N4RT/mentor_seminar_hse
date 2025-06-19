from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import IntegerType, StringType, BooleanType, FloatType

spark = SparkSession.builder.appName("CSV to Parquet ETL process").getOrCreate()

source_path = "s3a://etl-transfer-bucket/raw-data/reddit_opinion_climate_change.csv"
target_path = "s3a://data-proc-main-bucket/processed-data/reddit_opinion_climate_change.parquet"

try:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

    df = df.withColumn("comment_id", col("comment_id").cast(StringType())) \
        .withColumn("score", col("score").cast(FloatType())) \
        .withColumn("self_text", col("self_text").cast(StringType())) \
        .withColumn("subreddit", col("subreddit").cast(StringType())) \
        .withColumn("created_time", to_timestamp(col("created_time").cast("string"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("post_id", col("post_id").cast(StringType())) \
        .withColumn("controversiality", col("controversiality").cast(FloatType())) \
        .withColumn("ups", col("ups").cast(IntegerType())) \
        .withColumn("downs", col("downs").cast(IntegerType())) \
        .withColumn("user_is_verified", col("user_is_verified").cast(BooleanType())) \
        .withColumn("user_account_created_time",
                    to_timestamp(col("user_account_created_time").cast("string"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("user_awardee_karma", col("user_awardee_karma").cast(FloatType())) \
        .withColumn("user_awarder_karma", col("user_awarder_karma").cast(FloatType())) \
        .withColumn("user_link_karma", col("user_link_karma").cast(FloatType())) \
        .withColumn("user_comment_karma", col("user_comment_karma").cast(FloatType())) \
        .withColumn("user_total_karma", col("user_total_karma").cast(FloatType())) \
        .withColumn("post_score", col("post_score").cast(IntegerType())) \
        .withColumn("post_self_text", col("post_self_text").cast(StringType())) \
        .withColumn("post_title", col("post_title").cast(StringType())) \
        .withColumn("post_upvote_ratio", col("post_upvote_ratio").cast(FloatType())) \
        .withColumn("post_thumbs_ups", col("post_thumbs_ups").cast(IntegerType())) \
        .withColumn("post_total_awards_received", col("post_total_awards_received").cast(IntegerType())) \
        .withColumn("post_created_time",
                    to_timestamp(col("post_created_time").cast("string"), "yyyy-MM-dd HH:mm:ss"))

    df = df.dropna()
    df = df.dropDuplicates()

    df.write.mode("overwrite").parquet(target_path)

    print("Task completed")

except Exception as e:
    print("Exception thrown:", e)

spark.stop()
