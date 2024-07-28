import shutil


from configparser import ConfigParser
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc, col, lower, regexp_replace, udf, trim, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Remove the previous checkpoints if present
shutil.rmtree('./tmp', ignore_errors=True)


# Config parsing
_config = ConfigParser()
_config.read(["./config/config.ini"])
bootstrapServers = _config.get("KAFKA", "bootstrap_servers")
input_topic = _config.get("TOPICS", "input_topic")
output_topic = _config.get("TOPICS", "output_topic")

# Initialize spark
spark = (
    SparkSession
    .builder
    .appName("media-bias-detection")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("./tmp")


def random_value(values):
    import random
    return random.choice(values)


@udf()
def predict_sentiment(x):
    return random_value(["positive", "negative", "neutral"])


@udf()
def predict_category(x):
    return random_value(["buisness", "tech", "political", "global", "entertainment"])


@udf()
def predict_leaning(x):
    return random_value(["left", "right", "center"])


@udf(returnType=ArrayType(StringType()))
def tags_to_list(x):
    return x.strip('][').split(', ')


if __name__ == "__main__":
    # TODO: change to kafka streaming
    """
    # Read stream from Kafka
    data = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", input_topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )
    """

    # Reading from CSV file
    schema = StructType([
        StructField("url", StringType(), True),
        StructField("date", StringType(), True),
        StructField("title", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("heading", StringType(), True),
        StructField("source", StringType(), True),
        StructField("text", StringType(), True),
        StructField("bias_rating", StringType(), True)
    ])
    # Create a filter condition for non-null values for all columns
    filter_condition = reduce(
        lambda a, b: a & b,
        [col(c).isNotNull() for c in ["heading"]]
    )
    data = (
        spark
        .readStream
        .option("sep", "|")
        .option("multiLine", "true")
        .option("header", "true")
        .schema(schema)
        .csv("./scraper/data/")
        .filter(filter_condition)
        .select(col("heading").alias("value"))
    )

    filtered_stream_df = (
        data
        .select(
            col("value").alias("heading"),
            predict_sentiment(col("value")).alias("sentiment"),
            predict_category(col("value")).alias("category"),
            predict_leaning(col("value")).alias("bias_rating")
        )
    )

    """
    # Output to console
    query = filtered_stream_df \
        .writeStream \
        .outputMode('append') \
        .format('console') \
        .start()
    query.awaitTermination()
    """

    # Output to Kafka topic further connected to ELK
    query = (
        filtered_stream_df
        .select(
            to_json(
                struct(
                    col("heading"),
                    col("sentiment"),
                    col("category"),
                    col("bias_rating")
                )
            ).alias("value"))
        .writeStream
        .format("kafka")
        .outputMode("update")
        .option("checkpointLocation", "./tmp")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("topic", output_topic)
        .start()
    )

    # Wrap up
    query.awaitTermination()
    spark.stop()
