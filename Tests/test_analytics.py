import pytest

from ingestion.analytics import compute_buyer_origin_premium

LISBOA_ROWS = [
    {"indicator": "0012239", "period": "1.º Trimestre de 2024", "year": 2024, "sub_period": 1,
     "geo_code": "1106", "geo_name": "Lisboa", "dim_3": "1", "dim_3_label": "Territorio nacional",
     "value": 4000.0, "display_value": "4 000"},
    {"indicator": "0012239", "period": "1.º Trimestre de 2024", "year": 2024, "sub_period": 1,
     "geo_code": "1106", "geo_name": "Lisboa", "dim_3": "2", "dim_3_label": "Estrangeiro",
     "value": 6000.0, "display_value": "6 000"},
    {"indicator": "0012239", "period": "2.º Trimestre de 2024", "year": 2024, "sub_period": 2,
     "geo_code": "1106", "geo_name": "Lisboa", "dim_3": "1", "dim_3_label": "Territorio nacional",
     "value": 4200.0, "display_value": "4 200"},
    {"indicator": "0012239", "period": "2.º Trimestre de 2024", "year": 2024, "sub_period": 2,
     "geo_code": "1106", "geo_name": "Lisboa", "dim_3": "2", "dim_3_label": "Estrangeiro",
     "value": 5250.0, "display_value": "5 250"},
]


class FakeINEClient:
    """Stands in for INEClient.to_dataframe: same region/dim3 filtering
    behavior, but serving fabricated rows instead of a live INE call."""

    def __init__(self, spark, rows):
        self._spark = spark
        self._rows = rows

    def to_dataframe(self, name_or_varcd, region=None, dim3=None, dim1=None,
                      start_year=None, end_year=None):
        df = self._spark.createDataFrame(self._rows)
        if region is not None:
            df = df.filter(df.geo_name == region)
        if dim3 is not None:
            df = df.filter(df.dim_3 == dim3)
        return df


def test_compute_buyer_origin_premium_computes_pct_per_period(spark):
    client = FakeINEClient(spark, LISBOA_ROWS)

    df = compute_buyer_origin_premium(client, region="Lisboa", start_year=2024, end_year=2024)
    rows = {r["period"]: r.asDict() for r in df.collect()}

    assert rows["1.º Trimestre de 2024"]["national_price_per_m2"] == 4000.0
    assert rows["1.º Trimestre de 2024"]["foreign_price_per_m2"] == 6000.0
    assert rows["1.º Trimestre de 2024"]["foreign_premium_pct"] == pytest.approx(50.0)

    assert rows["2.º Trimestre de 2024"]["foreign_premium_pct"] == pytest.approx(25.0)


def test_compute_buyer_origin_premium_orders_chronologically(spark):
    client = FakeINEClient(spark, LISBOA_ROWS)

    df = compute_buyer_origin_premium(client, region="Lisboa", start_year=2024, end_year=2024)
    periods = [r["period"] for r in df.collect()]

    assert periods == ["1.º Trimestre de 2024", "2.º Trimestre de 2024"]


def test_compute_buyer_origin_premium_drops_periods_missing_either_side(spark):
    rows = LISBOA_ROWS + [{
        "indicator": "0012239", "period": "3.º Trimestre de 2024", "year": 2024, "sub_period": 3,
        "geo_code": "1106", "geo_name": "Lisboa", "dim_3": "1", "dim_3_label": "Territorio nacional",
        "value": 4300.0, "display_value": "4 300",
    }]  # no matching foreign-buyer row for Q3
    client = FakeINEClient(spark, rows)

    df = compute_buyer_origin_premium(client, region="Lisboa", start_year=2024, end_year=2024)
    periods = {r["period"] for r in df.collect()}

    assert periods == {"1.º Trimestre de 2024", "2.º Trimestre de 2024"}
