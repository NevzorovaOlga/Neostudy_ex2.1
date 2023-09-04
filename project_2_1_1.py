import findspark
findspark.init('/home/olga/spark-3.1.1-bin-hadoop3.2')
import pyspark

from pyspark.sql import SparkSession 


print("<<---***--- START ---***--->>")

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "athletics", "summer"),
    (2, "swimming", "summer"),
    (3, "gymnastics", "summer"),
    (4, "cycling", "summer"),
    (5, "tennis", "summer"),
    (6, "skiing", "winter"),
    (7, "figure Skating", "winter"),
    (8, "snowboarding", "winter"),
    (9, "bobsleigh", "winter"),
    (10, "curling", "winter")
]

schema = "row_id INT, discipline STRING, season STRING"

df = spark.createDataFrame(data, schema)

#df.write.option("header", "true").option("delimiter", "\t").csv("ex_2_1_1.csv")
df.coalesce(1).write.csv("ex_2_1_1.csv", sep="\t", header=True)

print("<<---***--- END ---***--->>")
