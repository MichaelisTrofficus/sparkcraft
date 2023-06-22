import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType


def add_salt_column(df: DataFrame, skew_factor: int):
    """
    Adds a salt column to a DataFrame. We will be using this salt column when we are trying to perform
    join, groupBy, etc. operations into a skewed DataFrame. The idea is to add a random column and use
    the original keys + this salted key to perform the operations, so that we can avoid data skewness and
    possibly, OOM errors.

    :param df: A Pyspark DataFrame
    :param skew_factor: The skew factor. For example, if we set this value to 3, then the salted column will
        be populated by the elements 0, 1 and 2, extracted from a uniform probability distribution.
    :return: The original DataFrame with a `salt_id` column.
    """
    return df.withColumn("salt_id", (sf.rand() * skew_factor).cast(IntegerType()))
