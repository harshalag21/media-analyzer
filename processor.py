import shutil
from functools import reduce

import sparknlp

from config.parsedconfig import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import PipelineModel

# spark-submit --packages com.johnsnowlabs.nlp:spark-nlp-silicon_2.12:5.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 processor.py

# Remove the previous checkpoints if present
shutil.rmtree('./tmp', ignore_errors=True)

# Load ML models
sentiment_analysis_model = (
    PipelineModel
    .load(sentiment_analysis_model_path)
)
"""category_detection_model = (
    PipelineModel
    .load(category_detection_model_path)
)
bias_detection_model = (
    PipelineModel
    .load(bias_detection_model_path)
)"""

# Initialize spark
"""spark = (
    SparkSession
    .builder
    .appName("media-bias-detection")
    .getOrCreate()
)"""
spark = sparknlp.start()
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
    return random_value(["business", "tech", "political", "global", "entertainment"])


@udf()
def predict_leaning(x):
    return random_value(["left", "right", "center"])


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
        .select(col("heading").alias("text"))
    )

    data = (
        sentiment_analysis_model
        .transform(data)
        .select(col("text").alias("value"), col("class.result").alias("sentiment"))
    )

    data = (
        data
        .select(
            col("value").alias("heading"),
            "sentiment",
            # predict_sentiment(col("value")).alias("sentiment"),
            predict_category(col("value")).alias("category"),
            predict_leaning(col("value")).alias("bias_rating")
        )
    )

    """
    # Output to console
    query = data \
        .writeStream \
        .outputMode('append') \
        .format('console') \
        .start()
    query.awaitTermination()
    """

    # Output to Kafka topic further connected to ELK
    query = (
        data
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
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", output_topic)
        .start()
    )

    # Wrap up
    query.awaitTermination()
    spark.stop()
