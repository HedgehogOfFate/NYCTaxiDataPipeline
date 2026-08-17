import os

from pyspark.sql import SparkSession

def get_spark(app_name: str) -> SparkSession:

    master = os.environ.get("SPARK_MASTER_URL", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
    )

    if master.startswith("local"):
        driver_host = os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1")
        builder = builder.config("spark.driver.host", driver_host).config("spark.driver.bindAddress", driver_host)
        builder = builder.config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))

    jdbc_jar = "/opt/airflow/jars/postgresql-42.7.3.jar"
    if os.path.exists(jdbc_jar):
        builder = builder.config("spark.jars", jdbc_jar)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def postgres_jdbc_options(dbtable: str) -> dict:
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_GOLD_DB", "gold")
    user = os.environ.get("POSTGRES_USER", "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow")
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{db}",
        "dbtable": dbtable,
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
