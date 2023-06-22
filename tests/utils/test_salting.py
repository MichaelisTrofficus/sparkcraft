import pytest

from sparkcraft.utils.salting import add_salt_column


@pytest.mark.parametrize("skew_factor", [1, 10, 100])
def test_add_salt_column(spark, random_uniform_df, skew_factor):
    df = random_uniform_df(spark, n_rows=1000, n_cols=1)
    df = add_salt_column(df, skew_factor=skew_factor)
    unique_salting_ids = [x[0] for x in df.select("salt_id").distinct().collect()]
    assert sorted(unique_salting_ids) == sorted(x for x in range(skew_factor))
