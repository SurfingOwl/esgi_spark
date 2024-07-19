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
df_no_repo_null = df.dropna(subset="repo")
df_no_repo_null.groupby("repo").count().sort("count", ascending=False).limit(10).show()

# Exercice 2
df_apache_spark = df_no_repo_null.filter(df.repo == "apache/spark").withColumn("date", to_date("date", "EEE MMM dd HH:mm:ss yyyy Z"))

print(df_apache_spark.groupby("author").count().sort("count", ascending=False).first())

# Exercice 3
today = datetime.datetime.now()
five_years_ago = today.replace(year=today.year - 5)

df_apache_spark \
    .dropna(subset="date") \
    .where(col("date") >= five_years_ago) \
    .groupby("author") \
    .count() \
    .sort("count", ascending=False) \
    .show()

