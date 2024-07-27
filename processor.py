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


@udf(returnType=ArrayType(StringType()))
def tags_to_list(x):
    return x.strip('][').split(', ')


if __name__ == "__main__":
    # TODO: change to kafka streaming
    """
    # Read stream from Kafka
    articles = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", input_topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )
    """
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

    data = (
        spark
        .readStream
        .option("sep", "|")
        .option("multiLine", "true")
        .option("header", "true")
        .schema(schema)
        .csv("./scraper/data/")
    )
    columns = data.columns

    # Create a filter condition for non-null values for all columns
    filter_condition = reduce(
        lambda a, b: a & b,
        [col(c).isNotNull() for c in columns]
    )
    filtered_stream_df = (
        data
        .filter(filter_condition)
        .select(
            tags_to_list(col("tags")).alias("tag_list"),
            "heading",
            "text",
            "bias_rating"
        )
    )
    """ query = filtered_stream_df \
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
                    col("tag_list"),
                    col("heading"),
                    col("text"),
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
