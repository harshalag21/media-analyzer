import shutil

import mlflow
import sparknlp
from config.parsedconfig import *
from pyspark.sql.functions import col, udf, to_json, struct
from pyspark.ml import PipelineModel


# Remove the previous checkpoints if present
shutil.rmtree('./tmp', ignore_errors=True)

# Load ML models
sentiment_analysis_model = (
    PipelineModel
    .load(sentiment_analysis_model_path)
)
category_detection_model = (
    mlflow
    .pyfunc
    .load_model(category_detection_model_path)
)
bias_detection_model = (
    PipelineModel
    .load(bias_detection_model_path)
)

# Initialize spark
spark = sparknlp.start()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("./tmp")


@udf()
def predict_category(x):
    try:
        return category_detection_model.predict(str(x)).to_dict()['label'][0]
    except mlflow.exceptions.MlflowException as e:
        print(f"Mlflow Exception: {e}")
        return "General"


if __name__ == "__main__":
    # Read stream from Kafka
    data = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(col("value").alias("text"))
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
    """

    # Sentiment prediction
    data = (
        sentiment_analysis_model
        .transform(data)
        .select(
            "text",
            col("class.result").alias("sentiment")
        )
    )

    # Bias rating prediction
    data = (
        bias_detection_model
        .transform(data)
        .select(
            col("text").alias("heading"),
            "sentiment",
            col("class.result").alias("bias_rating")
        )
    )

    # Category prediction
    data = (
        data
        .select(
            "heading",
            "sentiment",
            "bias_rating",
            predict_category(col("heading")).alias("category"),
        )
    )

    """
    # Output to console
    query = (
        data 
        .writeStream 
        .outputMode('append')
        .format('console') 
        .start()
    )
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
