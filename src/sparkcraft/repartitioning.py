import logging
import math
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql._typing import ColumnOrName
from utils.partitions import get_quantile_partition_count
from utils.size_estimation import df_size_in_bytes_approximate
from utils.size_estimation import df_size_in_bytes_exact


def safe_repartition(
    df: DataFrame,
    numPartitions: Union[int, "ColumnOrName"],
):
    """
    Acts as a safe repartition function. If we are trying to repartition by a number of partitions `numPartitions` less
    than the number of current partitions, it will automatically make a coalesce operation.

    Note: This method will only be useful if we DO NOT WANT TO REPARTITION BY ANY OTHER COLUMN. If we want to
    reduce the number of partitions but still maintain partitions by some columns, we will need to use the
    usual `df.repartition(...)`

    :param df: A Pyspark DataFrame
    :param numPartitions: Number of partitions
    :return: A partitioned Dataframe
    """
    if df.rdd.getNumPartitions() < numPartitions:
        df = df.repartition(numPartitions)
    elif df.rdd.getNumPartitions() > numPartitions:
        df = df.coalesce(numPartitions)
    return df


def repartition_with_size_estimation(
    df: DataFrame,
    partition_cols: Union[str, List[str]] = None,
    df_sample_perc: float = 0.05,
    target_size_in_bytes: int = 134_217_728,
    quantile_count_estimation: float = 0.5,
):
    # TODO: Improve this docstring ...
    """
    This method repartitions a PySpark DataFrame using size estimation.
    :param df: A PySpark DataFrame
    :param partition_cols: List of partition cols
    :param df_sample_perc: A float representing the percentage of the input DataFrame that will be used
        to estimate the total size.
    :param target_size_in_bytes: The target size of the future partitions. Defaults to 128 MB
    :param quantile_count_estimation: The quantile ...
    :return: A partitioned DataFrame
    """
    if df_sample_perc <= 0 or df_sample_perc > 1:
        raise ValueError("df_sample_perc must be in the interval (0, 1]")

    if df_sample_perc == 1:
        logging.info("Using complete DataFrame")
        df_size_in_bytes = df_size_in_bytes_exact(df)
    else:
        logging.info(
            f"Using sampling percentage {df_sample_perc}."
            f" Notice the DataFrame size is just an approximation"
        )
        df_size_in_bytes = df_size_in_bytes_approximate(df, sample_perc=df_sample_perc)

    if not partition_cols:
        n_partitions = math.ceil(df_size_in_bytes / target_size_in_bytes)
    else:
        n_rows = get_quantile_partition_count(df, quantile=quantile_count_estimation)
        percentile_partition_size = (df_size_in_bytes / df.count()) * n_rows
        n_partitions = df_size_in_bytes / percentile_partition_size

    logging.info(
        f"DataFrame Size: {df_size_in_bytes} | Number of partitions: {n_partitions}"
    )

    return df.repartition(n_partitions, partition_cols)
