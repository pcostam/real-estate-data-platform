from pyspark.sql.functions import col, max as spark_max

from ingestion.ine_client import INEClient


def get_yoy_change(client: INEClient, indicator: str, region: str | None = None):
    """Year-over-year % change per geo_code/dim_3, aligning periods that
    share the same sub_period (quarter/month) one year apart."""
    df = client.to_dataframe(indicator, region=region)

    current = df.select(
        col("geo_code"), col("geo_name"), col("dim_3"), col("dim_3_label"),
        col("year").alias("year"), col("sub_period").alias("sub_period"),
        col("period").alias("period"),
        col("value").cast("double").alias("value"),
    )
    previous = current.select(
        col("geo_code").alias("p_geo_code"),
        col("dim_3").alias("p_dim_3"),
        (col("year") + 1).alias("p_year"),
        col("sub_period").alias("p_sub_period"),
        col("value").alias("p_value"),
    )

    joined = current.join(
        previous,
        (current.geo_code == previous.p_geo_code)
        & (current.dim_3.eqNullSafe(previous.p_dim_3))
        & (current.year == previous.p_year)
        & (current.sub_period.eqNullSafe(previous.p_sub_period)),
        "inner",
    )

    return joined.select(
        col("geo_code"), col("geo_name"), col("dim_3"), col("dim_3_label"),
        col("period"), col("value"), col("p_value").alias("prior_year_value"),
        ((col("value") - col("p_value")) / col("p_value") * 100).alias("yoy_pct_change"),
    )


def compare_regions(client: INEClient, indicator: str, regions: list[str], period: str = "latest"):
    """One row per region for a given period (or the latest available)."""
    df = client.to_dataframe(indicator)

    if period == "latest":
        latest = df.select(spark_max("period").alias("period")).collect()[0]["period"]
        df = df.filter(col("period") == latest)
    else:
        df = df.filter(col("period") == period)

    return df.filter(col("geo_name").isin(regions))


def compute_price_to_income(
    client: INEClient,
    price_varcd_or_name: str,
    income_varcd: str,
    region: str,
    dwelling_size_m2: float,
) -> dict:
    """Price-to-income ratio: (price per m2 * dwelling size) / annual income,
    using the latest available period for each series in the given region."""
    price_df = client.to_dataframe(price_varcd_or_name, region=region)
    income_df = client.to_dataframe(income_varcd, region=region)

    price_row = (
        price_df.orderBy(col("period").desc()).limit(1).collect()
    )
    income_row = (
        income_df.orderBy(col("period").desc()).limit(1).collect()
    )

    if not price_row:
        raise ValueError(f"No data for {price_varcd_or_name!r} in region {region!r}")
    if not income_row:
        raise ValueError(f"No data for income indicator {income_varcd!r} in region {region!r}")

    price_per_m2 = float(price_row[0]["value"])
    annual_income = float(income_row[0]["value"])

    return {
        "region": region,
        "price_period": price_row[0]["period"],
        "income_period": income_row[0]["period"],
        "price_per_m2": price_per_m2,
        "dwelling_size_m2": dwelling_size_m2,
        "annual_income": annual_income,
        "price_to_income_ratio": (price_per_m2 * dwelling_size_m2) / annual_income,
    }
