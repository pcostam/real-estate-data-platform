from ingestion.ine_client import INEClient


client = INEClient()

df = client.to_dataframe("median_price_per_m2")

df.show()
