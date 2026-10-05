from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
import requests

from ingestion import ine_client
from ingestion.ine_client import INEClient, _is_retryable_error, _retry_after_seconds, parse_period


@pytest.mark.parametrize(
    "period, expected",
    [
        ("1.º Trimestre de 2023", (2023, 1)),
        ("4.º Trimestre de 2025", (2025, 4)),
        ("Janeiro de 2024", (2024, 1)),
        ("Março de 2024", (2024, 3)),
        ("Dezembro de 2024", (2024, 12)),
        ("2023", (2023, None)),
    ],
)
def test_parse_period_valid(period, expected):
    assert parse_period(period) == expected


def test_parse_period_unrecognized_format_raises():
    with pytest.raises(ValueError):
        parse_period("not a period")


def test_download_passes_varcd_as_query_param_not_url(monkeypatch):
    from ingestion import ine_client

    captured = {}

    def fake_get(url, params):
        captured["url"] = url
        captured["params"] = params
        return type("R", (), {"json": lambda self: [{}]})()

    monkeypatch.setattr(ine_client, "_get", fake_get)
    INEClient().download("0012239&Dim1=S5A20001")

    assert captured["url"] == ine_client.INE_INDICATOR_URL
    assert captured["params"]["varcd"] == "0012239&Dim1=S5A20001"
    assert "Dim1" not in captured["params"]


def test_to_dataframe_empty_payload_raises_clear_error(monkeypatch):
    client = INEClient()
    monkeypatch.setattr(
        client, "download", lambda name_or_varcd, dim1=None: {"IndicadorCod": "0012239", "Dados": {}}
    )

    with pytest.raises(ValueError, match="no data"):
        client.to_dataframe("0012239")


def _payload(records):
    return {"IndicadorCod": "0012239", "Dados": {"1.º Trimestre de 2024": records}}


def _stub_download(monkeypatch, client, records):
    monkeypatch.setattr(client, "download", lambda name_or_varcd, dim1=None: _payload(records))


LISBOA_BY_ORIGIN = [
    {"geocod": "1106", "geodsg": "Lisboa", "dim_3": "1", "dim_3_t": "Territorio nacional",
     "valor": "4000", "ind_string": "4 000"},
    {"geocod": "1106", "geodsg": "Lisboa", "dim_3": "2", "dim_3_t": "Estrangeiro",
     "valor": "6000", "ind_string": "6 000"},
]


def test_to_dataframe_unknown_region_raises_clear_error(monkeypatch):
    client = INEClient()
    _stub_download(monkeypatch, client, LISBOA_BY_ORIGIN)

    with pytest.raises(ValueError, match="region 'Lisbon'"):
        client.to_dataframe("0012239", region="Lisbon")


def test_to_dataframe_unmatched_dim3_raises_and_lists_available(monkeypatch):
    client = INEClient()
    _stub_download(monkeypatch, client, LISBOA_BY_ORIGIN)

    with pytest.raises(ValueError, match=r"dim3='9'.*\['1', '2'\]"):
        client.to_dataframe("0012239", dim3="9")


def test_to_dataframe_dim3_on_indicator_without_breakdown_raises(monkeypatch):
    client = INEClient()
    _stub_download(monkeypatch, client, [
        {"geocod": "1106", "geodsg": "Lisboa", "valor": "33", "ind_string": "33"},
    ])

    with pytest.raises(ValueError, match="no dim_3 breakdown"):
        client.to_dataframe("0012239", dim3="T")


def test_to_dataframe_handles_indicator_without_dim3_field(monkeypatch, spark):
    # e.g. number_of_sales: INE omits dim_3 entirely, so every row has None.
    client = INEClient()
    _stub_download(monkeypatch, client, [
        {"geocod": "1106", "geodsg": "Lisboa", "valor": "33", "ind_string": "33"},
        {"geocod": "1105", "geodsg": "Cascais", "valor": "12", "ind_string": "12"},
    ])

    rows = client.to_dataframe("0012239", region="Lisboa").collect()

    assert len(rows) == 1
    assert rows[0]["dim_3"] is None
    assert rows[0]["value"] == "33"
    assert rows[0]["year"] == 2024 and rows[0]["sub_period"] == 1


def test_rate_limiter_releases_permit_when_enter_is_interrupted(monkeypatch):
    from ingestion import ine_client

    limiter = ine_client._RateLimiter(max_per_second=1, max_concurrent=1)
    limiter._tokens = 0  # force the token wait, where the interrupt lands

    def interrupted_sleep(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr(ine_client.time, "sleep", interrupted_sleep)

    with pytest.raises(KeyboardInterrupt):
        with limiter:
            pass

    # The single permit must be free again, or every later request blocks forever.
    assert limiter._concurrency.acquire(blocking=False)


class _FakeResponse:
    def __init__(self, retry_after):
        self.headers = {} if retry_after is None else {"Retry-After": retry_after}


class _FakeHTTPError(Exception):
    def __init__(self, retry_after):
        self.response = _FakeResponse(retry_after)


def test_retry_after_numeric_seconds():
    assert _retry_after_seconds(_FakeHTTPError("7")) == 7.0


def test_retry_after_http_date():
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=60)
    delay = _retry_after_seconds(_FakeHTTPError(format_datetime(retry_at, usegmt=True)))
    assert 55 <= delay <= 60


def test_retry_after_http_date_in_past_is_zero():
    assert _retry_after_seconds(_FakeHTTPError("Wed, 21 Oct 2015 07:28:00 GMT")) == 0.0


@pytest.mark.parametrize("value", [None, "soon", ""])
def test_retry_after_missing_or_unparseable(value):
    assert _retry_after_seconds(_FakeHTTPError(value)) is None


def _http_error(status):
    response = requests.Response()
    response.status_code = status
    return requests.exceptions.HTTPError(response=response)


@pytest.mark.parametrize("exc, expected", [
    (requests.exceptions.ConnectionError(), True),
    (requests.exceptions.ReadTimeout(), True),
    (requests.exceptions.ConnectTimeout(), True),
    (_http_error(429), True),
    (_http_error(503), True),
    (_http_error(404), False),
    (ValueError(), False),
])
def test_is_retryable_error(exc, expected):
    assert _is_retryable_error(exc) is expected


def test_get_retries_after_dropped_connection(monkeypatch):
    calls = []
    ok = requests.Response()
    ok.status_code = 200

    def flaky_get(url, params, timeout):
        calls.append(url)
        if len(calls) == 1:
            raise requests.exceptions.ConnectionError("connection reset")
        return ok

    monkeypatch.setattr(ine_client._session, "get", flaky_get)
    monkeypatch.setattr(ine_client._get.retry, "sleep", lambda _seconds: None)

    assert ine_client._get("https://example.invalid", {}) is ok
    assert len(calls) == 2
