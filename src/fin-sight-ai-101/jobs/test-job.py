from pyspark.sql import SparkSession
from data_source.currency.currency_plugin import CurrencyDefaultDataSource

spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

# Register plugin
spark.dataSource.register(CurrencyDefaultDataSource)

# Use it
df = spark.read \
    .format("currency") \
    .option("startDate", "2025-01-01") \
    .option("endDate", "2025-01-10") \
    .schema("currency string, date string, rate double") \
    .load()

df.show()