from typing import List
from typing import Union

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def count_number_of_non_empty_partitions(iterator):
    """
    Simply returns de number of nonempty partitions in a DataFrame.
    :param iterator: An iterator containing each partition
    :return:
    """
    n = 0
    for _ in iterator:
        n += 1
        break
    yield n


def remove_empty_partitions(df: DataFrame):
    """
    This method will remove empty partitions from a DataFrame. It is useful after a filter, for
    example, when a great number of partitions may contain zero registers.

    Note: This functionality may be useless if you are using Adaptive Query Execution from Spark 3.0

    :param df: A pyspark DataFrame
    :return: A DataFrame with all empty partitions removed
    """
    non_empty_partitions = sum(
        df.rdd.mapPartitions(count_number_of_non_empty_partitions).collect()
    )
    return df.coalesce(non_empty_partitions)


def add_partition_id_column(df: DataFrame, partition_id_colname: str = "partition_id"):
    """
    Adds a column named `partition_id` to the input DataFrame which represents the partition id as
    output by `pyspark.sql.functions.spark_partition_id` method.
    :param df: A PySpark DataFrame
    :param partition_id_colname: The name of the column containing the partition id
    :return: The input DataFrame with an additional column (`partition_id`) which represents the partition id
    """
    return df.withColumn(partition_id_colname, sf.spark_partition_id())


def get_partition_count(df: DataFrame) -> DataFrame:
    """
    Gets the number of registers per partition. This method is useful if we are trying to determine if some
    partition is skewed.

    :return: A DataFrame containing `partition_id` and `count` columns
    """
    return add_partition_id_column(df).groupBy("partition_id").count()


def get_quantile_partition_count(
    df: DataFrame, quantile: float = 0.5, partition_cols: Union[str, List[str]] = None
):
    """
    It calculates the number of elements in the quantile of partitions. This will be a handy method
    for skewed data.

    :param df: A PySpark DataFrame
    :param quantile: The quantile provided. By default, the median
    :param partition_cols: If provided, the columns from which to make the grouping
    :return:
    """
    # Calculate approximate quantile for number of counts per partition keys
    return int(
        df.groupBy(*partition_cols)
        .count()
        .approxQuantile("count", [quantile], 0.001)[0]
    )
