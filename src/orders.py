from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.getOrCreate()

data = [
    (101, "Laptop", 2, 50000),
    (102, "Mouse", 5, 800),
    (103, "Keyboard", 3, 1500),
    (104, "Laptop", 1, 50000),
    (105, "Mouse", 4, 800)
]

columns = ["order_id", "product", "quantity", "price"]

df = spark.createDataFrame(data, columns)

print("========== ORIGINAL ORDERS ==========")
df.show()

df = df.withColumn(
    "revenue",
    col("quantity") * col("price")
)

print("========== ORDERS WITH REVENUE ==========")
df.show()

result = (
    df.groupBy("product")
      .agg(
          spark_sum("revenue").alias("total_revenue")
      )
      .orderBy(
          col("total_revenue").desc()
      )
)

print("========== PRODUCT-WISE REVENUE ==========")
result.show()

print("========== CI/CD DEMO VERSION 2 ==========")