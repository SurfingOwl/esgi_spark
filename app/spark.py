import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([StructField("commit", StringType(), False),
                     StructField("author", StringType(), False),
                     StructField("date", StringType(), False),
                     StructField("message", StringType(), False),
                     StructField("repo", StringType(), False),
                     ])

spark = SparkSession \
    .builder \
    .appName("git_prj") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

path = "/app/data/full.csv"

df = spark.read.csv(path, header=True, schema=schema)

# Exercice 1
# df.dropna(subset="repo").groupby("repo").count().sort("count", ascending=False).limit(10).show()

# Exercice 2
# df.filter(df.repo == "apache/spark").groupby("author").count().sort("count", ascending=False).show()

# Exercice 3
today = datetime.datetime.now()
five_years_ago = today.replace(year=today.year - 5)

df.withColumn("date", to_date("date", "EEE MMM dd HH:mm:ss yyyy Z")) \
    .dropna(subset="date") \
    .filter(df.repo == "apache/spark") \
    .where(col("date") >= five_years_ago) \
    .show()
    # .groupby("author") \
    # .count() \
    # .sort("count", ascending=False) \
