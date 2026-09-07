import re

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from ingestion.indicators import resolve_varcd

INE_INDICATOR_URL = "https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd={varcd}&lang=PT"

_MONTHS_PT = {
    "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5,
    "junho": 6, "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}
_QUARTER_RE = re.compile(r"(\d)\.\D+Trimestre de (\d{4})", re.IGNORECASE)
_MONTH_RE = re.compile(r"([A-Za-zÀ-ÿ]+) de (\d{4})")
_YEAR_RE = re.compile(r"^(\d{4})$")


def parse_period(period: str) -> tuple[int, int | None]:
    """Parse an INE period label into (year, sub_period), where sub_period
    is the quarter (1-4) or month (1-12) when present, else None."""
    match = _QUARTER_RE.search(period)
    if match:
        return int(match.group(2)), int(match.group(1))

    match = _MONTH_RE.search(period)
    if match:
        month_name = match.group(1).lower()
        month_name = (
            month_name.replace("ç", "c").replace("ã", "a")
        )
        month = _MONTHS_PT.get(month_name)
        if month:
            return int(match.group(2)), month

    match = _YEAR_RE.match(period.strip())
    if match:
        return int(match.group(1)), None

    raise ValueError(f"Unrecognized INE period format: {period!r}")


class INEClient:

    def __init__(self):
        self._spark = None

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = SparkSession.builder.appName("INE").getOrCreate()
        return self._spark

    def download(self, name_or_varcd: str) -> dict:
        varcd = resolve_varcd(name_or_varcd)
        url = INE_INDICATOR_URL.format(varcd=varcd)
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        return response.json()[0]

    def to_dataframe(
        self,
        name_or_varcd: str,
        region: str | None = None,
        dim3: str | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
    ):
        data = self.download(name_or_varcd)

        rows = []

        for period, records in data["Dados"].items():
            year, sub_period = parse_period(period)

            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue

            for record in records:
                rows.append({
                    "indicator": data["IndicadorCod"],
                    "period": period,
                    "year": year,
                    "sub_period": sub_period,
                    "geo_code": record.get("geocod"),
                    "geo_name": record.get("geodsg"),
                    "dim_3": record.get("dim_3"),
                    "dim_3_label": record.get("dim_3_t"),
                    "value": record.get("valor"),
                    "display_value": record.get("ind_string"),
                })

        df = self.spark.createDataFrame(rows)

        if region is not None:
            df = self.filter_region(df, region)
        if dim3 is not None:
            df = df.filter(col("dim_3") == dim3)

        return df

    @staticmethod
    def filter_region(df, region):
        return df.filter(col("geo_name") == region)
