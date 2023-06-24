from pyspark.sql import DataFrame


def df_size_in_bytes_exact(df: DataFrame):
    """
    Calculates the exact size in memory of a DataFrame by caching it and accessing the optimized plan

    Note: BE CAREFUL WITH THIS FUNCTION BECAUSE IT WILL CACHE ALL THE DATAFRAME!!! IF YOUR DATAFRAME IS
    TOO BIG USE `estimate_df_size_in_bytes`!!

    Args:
        df: A pyspark DataFrame

    Returns:
        The exact size in bytes
    """
    df.cache().count()
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    df.unpersist(blocking=True)
    return size_in_bytes


def df_size_in_bytes_approximate(df: DataFrame, sample_perc: float = 0.05):
    """
    This method takes a sample of the input DataFrame (`sample_perc`) and applies `df_size_in_bytes_exact`
    method to it. After it calculates the exact size of the sample, it extrapolates the total size.

    Args:
        df: A PySpark DataFrame
        sample_perc: The percentage of the DataFrame to sample. By default, a 5 %

    Raises:
        ValueError: If `sample_perc` is less than or equal to 0 or if it's greater than 1.

    Returns:
        The approximate size in bytes
    """
    if sample_perc <= 0 or sample_perc > 1:
        raise ValueError("`sample_perc` must be in the interval (0, 1]")

    sample_size_in_bytes = df_size_in_bytes_exact(df.sample(sample_perc))
    return sample_size_in_bytes / sample_perc
