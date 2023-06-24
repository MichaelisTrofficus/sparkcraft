import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType


def add_salt_column(df: DataFrame, skew_factor: int) -> DataFrame:
    """
    Adds a salt column to a DataFrame. We will be using this salt column when we are trying to perform
    join, groupBy, etc. operations into a skewed DataFrame. The idea is to add a random column and use
    the original keys + this salted key to perform the operations, so that we can avoid data skewness and
    possibly, OOM errors.

    Args:
        df: A PySpark DataFrame
        skew_factor: The skew factor. For example, if we set this value to 3, then the salted column will
            be populated by the elements 0, 1 and 2, extracted from a uniform probability distribution.

    Returns:
        The original DataFrame with a `salt_id` column.
    """
    return df.withColumn("salt_id", (sf.rand() * skew_factor).cast(IntegerType()))


def optimal_cross_join(df_bc: DataFrame, df: DataFrame) -> DataFrame:
    """
    A simple trick to solve the problem we have with CrossJoins between two dataframes,
    when the resulting partitions will be a multiplication of the initial partitions (e.g. if
    we make the cross join between `df1` and `df2` both with 100 partitions, then the resulting
    partitions will be 10_000).

    Args:
        df_bc: The DataFrame to be broadcasted
        df: The DataFrame

    Returns:
        The cross joined DataFrame
    """
    return sf.broadcast(df_bc).crossJoin(df)
