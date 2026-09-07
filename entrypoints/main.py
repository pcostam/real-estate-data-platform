import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from ingestion.ine_client import INEClient


client = INEClient()

df = client.to_dataframe("median_price_per_m2")

df.show()
