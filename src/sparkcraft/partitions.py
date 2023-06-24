import math
from typing import List
from typing import Union

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

from sparkcraft.utils.size_estimation import df_size_in_bytes_approximate
from sparkcraft.utils.size_estimation import df_size_in_bytes_exact


def remove_empty_partitions(df: DataFrame):
    """
    This method will remove empty partitions from a DataFrame. It is useful after a filter, for
    example, when a great number of partitions may contain zero registers.

    Note: This functionality may be useless if you are using Adaptive Query Execution from Spark 3.0

    Args:
        df: A pyspark DataFrame

    Returns:
        A DataFrame with all empty partitions removed
    """

    def _count_number_of_non_empty_partitions(iterator):
        """
        Simply returns de number of nonempty partitions in a DataFrame.

        Args:
            iterator: An iterator containing each partition

        Yields:
            The number of non empty partitions
        """
        n = 0
        for _ in iterator:
            n += 1
            break
        yield n

    non_empty_partitions = sum(
        df.rdd.mapPartitions(_count_number_of_non_empty_partitions).collect()
    )
    return df.coalesce(non_empty_partitions)


def add_partition_id_col(df: DataFrame, partition_id_colname: str = "partition_id"):
    """
    Adds a column named `partition_id` to the input DataFrame which represents the partition id as
    output by `pyspark.sql.functions.spark_partition_id` method.

    Args:
        df: A PySpark DataFrame
        partition_id_colname: The name of the column containing the partition id

    Returns:
        The input DataFrame with an additional column (`partition_id`) which represents the partition id
    """
    return df.withColumn(partition_id_colname, sf.spark_partition_id())


def get_partition_count_df(df: DataFrame) -> DataFrame:
    """
    Generates a DataFrame containing the number of elements for each partition. This method
    may be handy when trying to determine if data is skewed.

    Args:
        df: A PySpark DataFrame

    Returns:
        A DataFrame containing `partition_id` and `count` columns
    """
    return add_partition_id_col(df).groupBy("partition_id").count()


def get_partition_count_distribution(
    df: DataFrame, probabilities: List[float], relative_error: float = 0.0
) -> List[float]:
    """
    Generates a DataFrame containing

    Args:
        df: A PySpark DataFrame
        probabilities: The list of probabilities to be shown in the output DataFrame
        relative_error: The relative target precision. For more information, check PySpark's
            documentation about `approxQuantile` (https://spark.apache.org/docs/latest/api/python/
            reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html). Defaults to 0.
            to obtain exact quantiles, but if the operation is too expensive you can increase this
            value (although the quantile precision will diminish)

    Returns:
        A list containing the value for each probability.
    """
    partition_count_df = get_partition_count_df(df)
    return partition_count_df.approxQuantile(
        col="count", probabilities=probabilities, relativeError=relative_error
    )


def get_optimal_number_of_partitions(
    df: DataFrame,
    partition_cols: Union[str, List[str]] = None,
    df_sample_perc: float = None,
    target_size_in_bytes: int = 134_217_728,
    estimate_biggest_key_probability: float = 0.95,
    estimate_biggest_key_relative_error: float = 0.0,
):
    """
    This method calculated the optimal number of partitions for the input PySpark DataFrame `df`.

    Args:
        df: A PySpark DataFrame
        partition_cols: The columns, if provided, to partition the DataFrame by
        df_sample_perc: If provided, the sampling percentage for approximate size estimation
        target_size_in_bytes: The target size of each partition (~128MB)
        estimate_biggest_key_probability: In order to estimate the biggest key (that is, the partition cols key
            that contains the highest number of elements inside to estimate the size of the partitions).
        estimate_biggest_key_relative_error: The relative error of the `estimate_biggest_key_probability` estimation.
            Defaults to 0. (to obtain exact quantiles), but be careful with this since operation may be very expensive.

    Returns:
        The optimal number of partition for the given DataFrame
    """
    if not df_sample_perc:
        df_size_in_bytes = df_size_in_bytes_exact(df)
    else:
        print(
            f"Using sampling percentage {df_sample_perc}."
            f" Notice the DataFrame size is just an approximation"
        )
        df_size_in_bytes = df_size_in_bytes_approximate(df, sample_perc=df_sample_perc)

    if not partition_cols:
        n_partitions = math.ceil(df_size_in_bytes / target_size_in_bytes)

    else:
        partition_cols = (
            [partition_cols] if type(partition_cols) == str else partition_cols
        )

        # Calculate the number of elements per each partition cols grouping
        keys_count_df = df.groupBy(*partition_cols).count()
        n_unique_keys = keys_count_df.count()

        # In this case we will take a probability of 0.95 to assure that we can provide
        # reasonable estimates for partition sizes. Notice that, if your data is very skewed,
        # you could have problems with this method, since you'll still have very big partitions
        # that could generate OOM errors.
        n_rows_biggest_key = keys_count_df.approxQuantile(
            col="count",
            probabilities=[estimate_biggest_key_probability],
            relativeError=estimate_biggest_key_relative_error,
        )[0]

        biggest_partition_size = (df_size_in_bytes / df.count()) * n_rows_biggest_key

        # Careful here! If `biggest_partition_size` is greater than `target_size_in_bytes` you will get
        # one single partition. If the reason is that your partition is very big, then you'll have
        # OOM errors, and you should think about using broadcasting or maybe techniques such as salting.
        n_keys_per_partition = math.ceil(target_size_in_bytes / biggest_partition_size)
        n_partitions = math.ceil(n_unique_keys / n_keys_per_partition)

    return n_partitions
