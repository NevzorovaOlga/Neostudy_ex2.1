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
athletes_df = spark.read.csv("Athletes.csv", sep=";", header=True, inferSchema=True)

athletes_df.show()

athletes_df.createOrReplaceTempView("tmp_athletes")


schema_gen = "row_id INT, discipline STRING, season STRING"
gen_df = spark.read.csv("gen.csv", sep="\t", header=True, inferSchema=True)
gen_df.show()
gen_df.createOrReplaceTempView("tmp_gen")

athletes_df = spark.sql("""
SELECT tmp_athletes.discipline, count(*) as count_athletes_in_discipline 
FROM tmp_athletes JOIN tmp_gen ON lower(tmp_athletes.Discipline) = tmp_gen.discipline 
GROUP BY tmp_athletes.discipline
""")

athletes_df.show()

#df.write.format("parquet").save("ex_2_1_3.parquet")
athletes_df.coalesce(1).write.parquet("ex_2_1_3.parquet", mode="overwrite")

print("<<---***--- END ---***--->>")
