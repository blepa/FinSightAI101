from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType
import json
import requests

class StockIntradayDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options

    def read(self, partition):
        url = (
            "https://www.alphavantage.co/query"
            "?function=TIME_SERIES_INTRADAY"
            "&symbol=IBM"
            "&interval=5min"
            "&apikey=demo"
        )
        response = requests.get(url)
        response.raise_for_status()
        raw_json = response.json()
        yield (json.dumps(raw_json),)  # yield a single-row tuple with JSON string

class StockIntradayDataSource(DataSource):
    @classmethod
    def name(cls):
        return "json_raw"

    def schema(self):
        # Only one column, storing the JSON as a string
        return StructType([
            StructField("json", StringType(), False)
        ])

    def reader(self, schema):
        return StockIntradayDataSourceReader(schema, self.options)
