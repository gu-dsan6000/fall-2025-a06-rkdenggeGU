from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")  # 强制本地运行
    .appName("SparkLocalTest")
    .config("spark.driver.host", "127.0.0.1")  # 强制使用 localhost
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "100s")
    .getOrCreate()
)

print("✅ Spark 本地运行成功，版本:", spark.version)
spark.stop()
