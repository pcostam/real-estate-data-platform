import ingestion  # noqa: F401  (sets PYSPARK_* env vars before Spark starts)
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.appName("tests").getOrCreate()
    yield session
    session.stop()
