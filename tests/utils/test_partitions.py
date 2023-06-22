import pyspark.sql.functions as sf
import pytest

from sparkcraft.partitions import add_partition_id_column
from sparkcraft.partitions import get_partition_count
from sparkcraft.partitions import remove_empty_partitions


def test_count_number_of_empty_partitions(spark, random_uniform_df):
    df = random_uniform_df(spark, n_rows=10, n_cols=10).repartition(100)
    df_wo_empty_partitions = remove_empty_partitions(df)
    assert df_wo_empty_partitions.rdd.getNumPartitions() <= 10


@pytest.mark.parametrize(
    "partition_id_colname", ["partition_id", "test_partition_name"]
)
def test_add_partition_id_column(spark, random_uniform_df, partition_id_colname):
    df = random_uniform_df(spark, n_rows=100, n_cols=10).repartition(10)
    df = add_partition_id_column(df, partition_id_colname)
    unique_partition_ids = [
        x[0] for x in df.select(partition_id_colname).distinct().collect()
    ]
    assert unique_partition_ids == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_get_partition_count(spark, random_uniform_df):
    df = random_uniform_df(spark, n_rows=100, n_cols=1).repartition(10)
    partition_counts_df = get_partition_count(df)
    assert partition_counts_df.agg(sf.sum("count")).collect()[0][0] == 100
