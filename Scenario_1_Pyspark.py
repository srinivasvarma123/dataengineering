from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import os

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\kvrkr\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import *


data = [
    ('A', 'D', 'D'),
    ('B', 'A', 'A'),
    ('A', 'D', 'A')
]

df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")
df.show()

all_teams_df = df.select(
                        col("TeamA").alias("TeamName")).union(
                        df.select(col("TeamB").alias("TeamName"))
).distinct()
all_teams_df.show()

win_counts_df = (
                    df
                    .groupBy("Won")
                    .agg(count("Won").alias("WonCount"))
)

win_counts_df = win_counts_df.withColumnRenamed("Won", "TeamName")
win_counts_df.show()

result_df = (
    all_teams_df
    .join(win_counts_df, "TeamName", "left")
    .fillna({"WonCount": 0})
    .withColumnRenamed("WonCount", "Won")
    .orderBy("TeamName")
)
result_df.show()


