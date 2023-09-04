import findspark
findspark.init('/home/olga/spark-3.1.1-bin-hadoop3.2')
import pyspark

from pyspark.sql import SparkSession 


print("<<---***--- START ---***--->>")

spark = (SparkSession
 .builder
 .appName('pyspark_example')
 .enableHiveSupport()
 .getOrCreate())
 
schema = "Name STRING, NOC STRING, Discipline STRING"
df = spark.read.csv("Athletes.csv", sep=";", header=True, inferSchema=True)

df.show()

df.createOrReplaceTempView("tmp_athletes")

df = spark.sql("""
SELECT discipline, count(*) as count_athletes_in_discipline 
FROM tmp_athletes 
GROUP BY discipline
""")

df.show()

#df.write.format("parquet").save("ex_2_1_2.parquet")
df.coalesce(1).write.parquet("ex_2_1_2.parquet", mode="overwrite")

print("<<---***--- END ---***--->>")
