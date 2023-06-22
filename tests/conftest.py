import pyspark.sql.functions as sf
import pytest
from pyspark.sql import SparkSession

from sparkcraft.utils import get_spark_session


@pytest.fixture(scope="session", autouse=True)
def spark():
    spark = get_spark_session()
    return spark


@pytest.fixture(scope="session", autouse=True)
def random_uniform_df():
    def _random_uniform_df(spark: SparkSession, n_rows: int, n_cols: int, seed=32):
        """
        Generates a random uniform PySpark DataFrame
        :param spark: A Spark Session
        :param n_rows: Number of rows
        :param n_cols: Number of columns
        :param seed: Seed for sf.rand()
        :return:
        """
        df = spark.range(n_rows).select(sf.col("id"))
        df = df.select(
            "*", *(sf.rand(seed).alias("_" + str(target)) for target in range(n_cols))
        )
        return df.drop("id")

    return _random_uniform_df
