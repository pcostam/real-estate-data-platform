import os
import re
import threading
import time

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from requests.adapters import HTTPAdapter
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion.indicators import default_dim3, resolve_varcd

INE_INDICATOR_URL = "https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd={varcd}&lang=PT"
INE_META_URL = "https://www.ine.pt/ine/json_indicador/pindicaMeta.jsp?varcd={varcd}&lang=PT"

# Conservative, shared-citizen defaults for calling INE's public API. Every
# process using this client (including everyone else's, if this code is
# reused) throttles itself independently -- there's no central coordinator,
# so each instance must stay well under what a single IP could get away
# with. Overridable via env vars for anyone who knows their situation allows
# more (or needs less).
_MAX_REQUESTS_PER_SECOND = float(os.environ.get("INE_MAX_REQUESTS_PER_SECOND", "8"))
_MAX_CONCURRENT_REQUESTS = int(os.environ.get("INE_MAX_CONCURRENT_REQUESTS", "10"))


class _RateLimiter:
    """Thread-safe token bucket (request rate) + semaphore (concurrency cap).

    Shared across all INEClient instances in a process so the ceiling holds
    even if callers create multiple clients.
    """

    def __init__(self, max_per_second: float, max_concurrent: int):
        self._rate = max_per_second
        self._tokens = max_per_second
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._concurrency = threading.Semaphore(max_concurrent)

    def __enter__(self):
        self._concurrency.acquire()
        with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self._rate, self._tokens + (now - self._last_refill) * self._rate
                )
                self._last_refill = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return self
                time.sleep((1 - self._tokens) / self._rate)

    def __exit__(self, *exc_info):
        self._concurrency.release()


_rate_limiter = _RateLimiter(_MAX_REQUESTS_PER_SECOND, _MAX_CONCURRENT_REQUESTS)


def _is_retryable_http_error(exc: BaseException) -> bool:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status == 429 or (status is not None and 500 <= status < 600)


def _retry_after_seconds(exc: BaseException) -> float | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    value = response.headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def _wait_respecting_retry_after(retry_state):
    exc = retry_state.outcome.exception()
    retry_after = _retry_after_seconds(exc) if exc else None
    if retry_after is not None:
        return retry_after
    return wait_exponential(multiplier=1, min=1, max=30)(retry_state)


def _make_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=_MAX_CONCURRENT_REQUESTS, pool_maxsize=_MAX_CONCURRENT_REQUESTS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_session = _make_session()


@retry(
    retry=retry_if_exception(
        lambda exc: isinstance(exc, requests.exceptions.HTTPError) and _is_retryable_http_error(exc)
    ),
    wait=_wait_respecting_retry_after,
    stop=stop_after_attempt(5),
    reraise=True,
)
def _get(url: str) -> requests.Response:
    with _rate_limiter:
        response = _session.get(url, timeout=30)
    response.raise_for_status()
    return response

# INE's Dim1 (period) ordinal codes are YYYYMMDD-shaped, with MM/DD fixed
# per periodicity (e.g. quarters start 01/04/07/10-01). Subtracting exactly
# one year lands on the equivalent period a year earlier.
_ONE_YEAR_ORD = 10000

# Safety cap on how many individual period fetches a single start/end year
# range can trigger (INE's API returns one period per call).
_MAX_PERIOD_FETCHES = 60

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
        month_name = match.group(1).lower().replace("ç", "c").replace("ã", "a")
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

    def download(self, name_or_varcd: str, dim1: str | None = None) -> dict:
        """Fetch one period of an indicator: the latest, or a specific one
        via `dim1` (an INE period cat_id from get_metadata(), e.g. 'S5A20251')."""
        varcd = resolve_varcd(name_or_varcd)
        url = INE_INDICATOR_URL.format(varcd=varcd)
        if dim1:
            url += f"&Dim1={dim1}"
        response = _get(url)

        return response.json()[0]

    def get_metadata(self, name_or_varcd: str) -> dict:
        """Fetch indicator metadata, including every valid period's Dim1
        cat_id, label, and a sortable ordinal (YYYYMMDD-shaped int)."""
        varcd = resolve_varcd(name_or_varcd)
        url = INE_META_URL.format(varcd=varcd)
        response = _get(url)
        data = response.json()[0]

        periods = []
        for group in data["Dimensoes"]["Categoria_Dim"]:
            for records in group.values():
                for record in records:
                    if record.get("dim_num") == "1":
                        periods.append({
                            "cat_id": record["cat_id"],
                            "label": record["categ_dsg"],
                            "ord": int(record["categ_ord"]),
                        })
        periods.sort(key=lambda p: p["ord"])

        return {
            "varcd": data["IndicadorCod"],
            "periodic": data.get("Periodic"),
            "first_period": data.get("PrimeiroPeriodo"),
            "last_period": data.get("UltimoPeriodo"),
            "periods": periods,
        }

    def resolve_prior_year_period(self, name_or_varcd: str, period_label: str) -> dict | None:
        """Find the period metadata entry exactly one year before `period_label`,
        or None if the indicator's history doesn't reach back that far."""
        meta = self.get_metadata(name_or_varcd)
        current = next((p for p in meta["periods"] if p["label"] == period_label), None)
        if current is None:
            raise ValueError(f"Unknown period {period_label!r} for {name_or_varcd!r}")

        prior_ord = current["ord"] - _ONE_YEAR_ORD
        return next((p for p in meta["periods"] if p["ord"] == prior_ord), None)

    def _rows_from_payload(self, data: dict) -> list[dict]:
        rows = []
        for period, records in data["Dados"].items():
            year, sub_period = parse_period(period)
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
        return rows

    def to_dataframe(
        self,
        name_or_varcd: str,
        region: str | None = None,
        dim3: str | None = None,
        dim1: str | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
    ):
        """Fetch an indicator as a Spark DataFrame.

        - No dim1/start_year/end_year: a single fetch of the latest period.
        - dim1: a single fetch of that specific period (see get_metadata()).
        - start_year/end_year: enumerates matching periods via metadata and
          fetches each one individually (INE's API returns one period per
          call), capped at _MAX_PERIOD_FETCHES.
        """
        if dim3 is None:
            dim3 = default_dim3(name_or_varcd)

        if dim1 is not None:
            payloads = [self.download(name_or_varcd, dim1=dim1)]
        elif start_year is None and end_year is None:
            payloads = [self.download(name_or_varcd)]
        else:
            meta = self.get_metadata(name_or_varcd)
            selected = [
                p for p in meta["periods"]
                if (start_year is None or p["ord"] // 10000 >= start_year)
                and (end_year is None or p["ord"] // 10000 <= end_year)
            ]
            if not selected:
                raise ValueError(
                    f"No periods found for {name_or_varcd!r} between "
                    f"{start_year} and {end_year}"
                )
            if len(selected) > _MAX_PERIOD_FETCHES:
                raise ValueError(
                    f"{len(selected)} periods match {name_or_varcd!r} between "
                    f"{start_year} and {end_year}; narrow the range "
                    f"(max {_MAX_PERIOD_FETCHES} periods per call)."
                )
            payloads = [self.download(name_or_varcd, dim1=p["cat_id"]) for p in selected]

        rows = [row for payload in payloads for row in self._rows_from_payload(payload)]
        df = self.spark.createDataFrame(rows)

        if region is not None:
            df = self.filter_region(df, region)
        if dim3 is not None:
            df = df.filter(col("dim_3") == dim3)

        return df

    @staticmethod
    def filter_region(df, region):
        return df.filter(col("geo_name") == region)
