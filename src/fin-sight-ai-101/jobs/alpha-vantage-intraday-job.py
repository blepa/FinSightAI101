from pyspark.sql import SparkSession
from data_source.stock.alpha_vantage.intraday import StockIntradayDataSource

spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

# Register plugin
spark.dataSource.register(StockIntradayDataSource)

# Use it
df = spark.read \
    .format("json_raw") \
    .option("startDate", "2025-01-01") \
    .option("endDate", "2025-01-10") \
    .schema("json string") \
    .load()

df.show(truncate=False)