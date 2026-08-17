import pathlib
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "spark_jobs"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("pytest")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
