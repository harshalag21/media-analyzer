import shutil
from functools import reduce

import sparknlp
import mlflow
from pyspark.sql import SparkSession

from config.parsedconfig import *
from pyspark.sql.functions import col, udf, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml import PipelineModel
import findspark
findspark.init()
spark = sparknlp.start()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("./tmp")

df = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema","true")
    .csv("./models/data/news_category_test.csv")
    .toDF("label","text")
    .select("label", "text")
)
print(df.count())
model = mlflow.pyfunc.load_model("models/category-detection")


@udf()
def classify(x):
    return model.predict(x).to_dict()['label'][0]

test = df.select(df.text, df.label, classify(df.text).alias("prediction"))
test.show()
from sklearn.metrics import classification_report
test = test.toPandas()

print(classification_report(test['label'], test['prediction']))