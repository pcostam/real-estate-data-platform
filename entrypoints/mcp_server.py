import functools
import json
import logging
from dataclasses import asdict

import requests

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from ingestion import analytics
from ingestion.indicators import KNOWN_INDICATORS
from ingestion.ine_client import INEClient

mcp = MCPServer("ine-housing-data")
client = INEClient()
logger = logging.getLogger(__name__)


def _expected_errors(fn):
    """Re-raise ValueError and INE request failures as ToolError so the model
    sees the message.

    MCPServer treats any other exception as a crash and returns only
    `Error executing tool <name>`, hiding e.g. "no data in region X". Those
    unexpected exceptions are logged with their traceback to stderr (stdout
    carries the MCP protocol) so they can still be diagnosed.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ValueError as exc:
            raise ToolError(str(exc)) from exc
        except requests.exceptions.RequestException as exc:
            raise ToolError(
                f"Request to INE failed after retries ({type(exc).__name__}): {exc}"
            ) from exc
        except Exception:
            logger.exception("Unexpected error in tool %s", fn.__name__)
            raise
    return wrapper


def _rows(df) -> list[dict]:
    return [row.asDict() for row in df.collect()]


@mcp.tool()
def list_known_indicators() -> dict:
    """List the registry of known INE indicators, with their varcd and verified flag."""
    return {name: asdict(info) for name, info in KNOWN_INDICATORS.items()}


@mcp.tool()
@_expected_errors
def get_indicator(
    varcd: str,
    region: str | None = None,
    dim3: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[dict]:
    """Fetch any INE indicator by varcd (or by a KNOWN_INDICATORS name), optionally
    filtered by region name, dim_3 category code, and/or year range."""
    df = client.to_dataframe(varcd, region=region, dim3=dim3, start_year=start_year, end_year=end_year)
    return _rows(df)


@mcp.tool()
@_expected_errors
def get_indicator_raw(varcd: str) -> dict:
    """Debug tool: return the raw, unparsed INE JSON response for a varcd."""
    return client.download(varcd)


@mcp.tool()
@_expected_errors
def get_median_price_per_m2(
    region: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[dict]:
    """Median sale price per m2 of housing (EUR/m2), optionally filtered by region/year range."""
    df = client.to_dataframe("median_price_per_m2", region=region, start_year=start_year, end_year=end_year)
    return _rows(df)


@mcp.tool()
@_expected_errors
def get_number_of_sales(
    region: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[dict]:
    """Number of housing sales in the trailing 12 months, optionally filtered by region/year range."""
    df = client.to_dataframe("number_of_sales", region=region, start_year=start_year, end_year=end_year)
    return _rows(df)


@mcp.tool()
@_expected_errors
def get_yoy_change(indicator: str, region: str | None = None) -> list[dict]:
    """Year-over-year percent change for an indicator (by varcd or KNOWN_INDICATORS name)."""
    df = analytics.get_yoy_change(client, indicator, region=region)
    return _rows(df)


@mcp.tool()
@_expected_errors
def compare_regions(indicator: str, regions: list[str], period: str = "latest") -> list[dict]:
    """Side-by-side comparison of an indicator across regions for one period ('latest' by default)."""
    df = analytics.compare_regions(client, indicator, regions, period=period)
    return _rows(df)


@mcp.tool()
@_expected_errors
def compute_price_to_income(
    price_varcd_or_name: str,
    income_varcd: str,
    region: str,
    dwelling_size_m2: float,
) -> dict:
    """Affordability ratio: (price per m2 * dwelling size) / annual income, for a region.
    You must supply an income indicator's INE varcd — none is registered by default."""
    return analytics.compute_price_to_income(
        client, price_varcd_or_name, income_varcd, region, dwelling_size_m2
    )


@mcp.tool()
@_expected_errors
def get_buyer_origin_premium(
    region: str,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[dict]:
    """Per-period % premium foreign buyers pay over national buyers on
    median_price_per_m2 in a region (foreign vs national dim_3 categories).
    A price-gap proxy, not a foreign-buyer share -- INE publishes no
    transaction volume split by buyer origin, only median price paid."""
    df = analytics.compute_buyer_origin_premium(client, region, start_year=start_year, end_year=end_year)
    return _rows(df)


@mcp.tool()
@_expected_errors
def get_listing_price_context(
    html_path: str,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict:
    """Parse a saved idealista listing page (local .html path) and return its
    street/neighbourhood/parish/municipality plus INE median price per m2 for
    the municipality. The price is municipality-level -- INE has no parish
    prices. parish_mismatch flags when the agency's link names a different
    parish than idealista does."""
    try:
        with open(html_path, encoding="utf-8") as f:
            html = f.read()
    except OSError as exc:
        raise ValueError(f"Cannot read listing page {html_path!r}: {exc}") from exc
    return analytics.get_listing_price_context(client, html, start_year=start_year, end_year=end_year)


@mcp.resource("ine://indicators")
def indicators_resource() -> str:
    """The known-indicator registry as JSON."""
    return json.dumps({name: asdict(info) for name, info in KNOWN_INDICATORS.items()}, indent=2)


@mcp.resource("ine://indicator/{varcd}")
def indicator_resource(varcd: str) -> str:
    """Any indicator's full parsed series (raw INE JSON) by varcd."""
    return json.dumps(client.download(varcd), indent=2)


if __name__ == "__main__":
    mcp.run()
