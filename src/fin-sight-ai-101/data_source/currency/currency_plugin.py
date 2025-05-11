from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

class CurrencyDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options

    def read(self, partition):
        import requests
        import json
        start_date = self.options.get("startDate", "2025-01-01")
        end_date = self.options.get("endDate", "2025-01-31")
        url = f"https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod={start_date}&endPeriod={end_date}&format=jsondata"
        response = requests.get(url)
        response.raise_for_status()
        data = json.loads(response.text)
        observations = data['dataSets'][0]['series']['0:0:0:0:0']['observations']
        time_periods = data['structure']['dimensions']['observation'][0]['values']
        for i, time in enumerate(time_periods):
            date = time['id']
            rate = observations.get(str(i), [None])[0]
            if rate is not None:
                yield ("USD", date, rate)

class CurrencyDefaultDataSource(DataSource):
    @classmethod
    def name(cls):
        return "currency"

    def schema(self):
        return "currency string, date string, rate double"

    def reader(self, schema):
        return CurrencyDataSourceReader(schema, self.options)
