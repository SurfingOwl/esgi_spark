from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_sub, current_date, explode, split, col, lower
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([StructField("commit", StringType(), False),
                     StructField("author", StringType(), False),
                     StructField("date", TimestampType(), False),
                     StructField("message", StringType(), False),
                     StructField("repo", StringType(), False),
                     ])

spark = SparkSession \
    .builder \
    .appName("git_prj") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

path = "/app/data/full.csv"

df = spark.read.csv(path, header=True, schema=schema)

# Exercice 1
#df.dropna(subset="repo").groupby("repo").count().sort("count", ascending=False).limit(10).show()

# Exercice 2
df.filter(df.repo == "apache/spark").groupby("author").count().sort("count", ascending=False).show()