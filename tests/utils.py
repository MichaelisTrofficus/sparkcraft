from pyspark.sql import SparkSession
import pyspark.sql.functions as sf


def generate_random_uniform_df(spark: SparkSession, n_rows: int, n_cols: int, seed=32):
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
