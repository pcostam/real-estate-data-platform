from pyspark.sql.functions import col, length

from ingestion.idealista_parser import parse_listing_location
from ingestion.ine_client import INEClient

# INE municipality geo_codes are 7 characters (NUTS prefix + 4-digit DICOFRE,
# e.g. Lisboa = "1A01106"); NUTS regions use shorter codes.
_MUNICIPALITY_GEO_CODE_LEN = 7


def get_yoy_change(client: INEClient, indicator: str, region: str | None = None):
    """Year-over-year % change per geo_code/dim_3: latest period vs. the
    matching period one year earlier."""
    current_df = client.to_dataframe(indicator, region=region)
    current_rows = current_df.take(1)
    if not current_rows:
        raise ValueError(f"No data for {indicator!r}" + (f" in region {region!r}" if region else ""))
    latest_period = current_rows[0]["period"]

    prior_period = client.resolve_prior_year_period(indicator, latest_period)
    if prior_period is None:
        raise ValueError(
            f"No prior-year data available for {indicator!r} (latest period is {latest_period!r})"
        )
    prior_df = client.to_dataframe(indicator, region=region, dim1=prior_period["cat_id"])

    current = current_df.select(
        col("geo_code"), col("geo_name"), col("dim_3"), col("dim_3_label"),
        col("period"), col("value").cast("double").alias("value"),
    )
    prior = prior_df.select(
        col("geo_code").alias("p_geo_code"),
        col("dim_3").alias("p_dim_3"),
        col("period").alias("prior_period"),
        col("value").cast("double").alias("prior_year_value"),
    )

    joined = current.join(
        prior,
        (current.geo_code == prior.p_geo_code) & (current.dim_3.eqNullSafe(prior.p_dim_3)),
        "inner",
    )

    return joined.select(
        col("geo_code"), col("geo_name"), col("dim_3"), col("dim_3_label"),
        col("period"), col("value"), col("prior_period"), col("prior_year_value"),
        ((col("value") - col("prior_year_value")) / col("prior_year_value") * 100).alias("yoy_pct_change"),
    )


def compare_regions(client: INEClient, indicator: str, regions: list[str], period: str = "latest"):
    """One row per region for a given period label (or the latest available)."""
    dim1 = None
    if period != "latest":
        meta = client.get_metadata(indicator)
        match = next((p for p in meta["periods"] if p["label"] == period), None)
        if match is None:
            raise ValueError(f"Unknown period {period!r} for indicator {indicator!r}")
        dim1 = match["cat_id"]

    df = client.to_dataframe(indicator, dim1=dim1)
    return df.filter(col("geo_name").isin(regions))


def compute_buyer_origin_premium(
    client: INEClient,
    region: str,
    price_varcd_or_name: str = "median_price_per_m2",
    foreign_dim3: str = "2",
    national_dim3: str = "1",
    start_year: int | None = None,
    end_year: int | None = None,
):
    """Per-period % premium foreign buyers pay over national buyers for the
    same indicator/region (foreign_price - national_price) / national_price.

    This is a price-gap signal, not a foreign-buyer *share*: INE does not
    publish transaction counts/volume split by buyer origin for this
    indicator, only the median price paid by each group. A widening premium
    is a candidate speculation-adjacent proxy (e.g. foreign demand bidding
    up a segment faster than the local market) -- not a validated detector,
    since the same pattern could just mean foreign buyers purchase larger
    or better-located units on average.
    """
    national_df = client.to_dataframe(
        price_varcd_or_name, region=region, dim3=national_dim3,
        start_year=start_year, end_year=end_year,
    )
    foreign_df = client.to_dataframe(
        price_varcd_or_name, region=region, dim3=foreign_dim3,
        start_year=start_year, end_year=end_year,
    )

    national = national_df.select(
        col("geo_code"), col("geo_name"),
        col("year"), col("sub_period"), col("period"),
        col("value").cast("double").alias("national_price_per_m2"),
    )
    foreign = foreign_df.select(
        col("geo_code").alias("f_geo_code"),
        col("year").alias("f_year"), col("sub_period").alias("f_sub_period"),
        col("value").cast("double").alias("foreign_price_per_m2"),
    )

    joined = national.join(
        foreign,
        (national.geo_code == foreign.f_geo_code)
        & (national.year == foreign.f_year)
        & (national.sub_period.eqNullSafe(foreign.f_sub_period)),
        "inner",
    )

    return joined.select(
        col("geo_code"), col("geo_name"),
        col("period"), col("year"), col("sub_period"),
        col("national_price_per_m2"), col("foreign_price_per_m2"),
        ((col("foreign_price_per_m2") - col("national_price_per_m2"))
         / col("national_price_per_m2") * 100).alias("foreign_premium_pct"),
    ).orderBy(col("geo_code"), col("year"), col("sub_period"))


def _single_row(df, label: str):
    rows = df.take(2)
    if not rows:
        raise ValueError(f"No data for {label}")
    if len(rows) > 1:
        raise ValueError(
            f"Multiple rows for {label}; the series is ambiguous (e.g. several dim_3 categories)"
        )
    return rows[0]


def compute_price_to_income(
    client: INEClient,
    price_varcd_or_name: str,
    income_varcd: str,
    region: str,
    dwelling_size_m2: float,
) -> dict:
    """Price-to-income ratio: (price per m2 * dwelling size) / annual income,
    using the latest available period for each series in the given region."""
    price_row = _single_row(
        client.to_dataframe(price_varcd_or_name, region=region),
        f"{price_varcd_or_name!r} in region {region!r}",
    )
    income_row = _single_row(
        client.to_dataframe(income_varcd, region=region),
        f"income indicator {income_varcd!r} in region {region!r}",
    )

    price_per_m2 = float(price_row["value"])
    annual_income = float(income_row["value"])

    return {
        "region": region,
        "price_period": price_row["period"],
        "income_period": income_row["period"],
        "price_per_m2": price_per_m2,
        "dwelling_size_m2": dwelling_size_m2,
        "annual_income": annual_income,
        "price_to_income_ratio": (price_per_m2 * dwelling_size_m2) / annual_income,
    }


def get_listing_price_context(
    client: INEClient,
    listing_html: str,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict:
    """Location of a saved idealista listing plus INE median_price_per_m2 for
    its municipality (latest period, or a year range).

    INE publishes this indicator down to municipality level only, so the
    price is the municipality's, never the listing's parish.
    """
    location = parse_listing_location(listing_html)
    df = client.to_dataframe(
        "median_price_per_m2", region=location.municipality,
        start_year=start_year, end_year=end_year,
    )
    df = df.filter(length(col("geo_code")) == _MUNICIPALITY_GEO_CODE_LEN)
    prices = [
        {
            "period": r["period"],
            "geo_code": r["geo_code"],
            "geo_name": r["geo_name"],
            "price_per_m2": float(r["value"]),
        }
        for r in df.orderBy(col("year"), col("sub_period")).collect()
    ]
    if not prices:
        raise ValueError(
            f"No municipality-level median_price_per_m2 for {location.municipality!r}"
        )

    return {
        "ad_id": location.ad_id,
        "street": location.street,
        "neighbourhood": location.neighbourhood,
        "parish": location.parish,
        "municipality": location.municipality,
        "agency_parish": location.agency_parish,
        "parish_mismatch": location.parish_mismatch,
        "price_geography": "municipality",
        "prices": prices,
    }
