import logging

from pyspark.sql import DataFrame


def df_size_in_bytes_exact(df: DataFrame):
    """
    Calculates the exact size in memory of a DataFrame by caching it and accessing the optimized plan

    NOTE: BE CAREFUL WITH THIS FUNCTION BECAUSE IT WILL CACHE ALL THE DATAFRAME!!! IF YOUR DATAFRAME IS
    TOO BIG USE `estimate_df_size_in_bytes`!!

    :param df: A pyspark DataFrame
    :return: The exact size in bytes
    """
    df = df.cache().select(
        df.columns
    )  # Just force the Spark planner to add the Cache op to the plan
    logging.info(f"Number of rows in the input DataFrame: {df.count()}")
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    df.unpersist(blocking=True)
    return size_in_bytes


def df_size_in_bytes_approximate(df: DataFrame, sample_perc: float = 0.05):
    """
    This method takes a sample of the input DataFrame (`sample_perc`) and applies `df_size_in_bytes_exact`
    method to it. After it calculates the exact size of the sample, it extrapolates the total size.

    :param df: A PySpark DataFrame
    :param sample_perc: The percentage of the DataFrame to sample. By default, a 5 %
    :return: The estimated size in bytes
    """
    sample_size_in_bytes = df_size_in_bytes_exact(df.sample(sample_perc))
    return sample_size_in_bytes / sample_perc
