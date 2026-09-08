import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.appName("tests").getOrCreate()
    yield session
    session.stop()
