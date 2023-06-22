import pytest

from sparkcraft.utils.size_estimation import df_size_in_bytes_approximate
from sparkcraft.utils.size_estimation import df_size_in_bytes_exact


def test_df_size_in_bytes_exact(spark, random_uniform_df):
    df = random_uniform_df(spark, n_rows=1000, n_cols=1)
    size_in_bytes = df_size_in_bytes_exact(df)
    assert size_in_bytes == 8000


@pytest.mark.parametrize("sample_perc", [0.1, 0.5])
def test_df_size_in_bytes_approximate(spark, random_uniform_df, sample_perc):
    df = random_uniform_df(spark, n_rows=100000, n_cols=1)
    size_in_bytes = df_size_in_bytes_approximate(df, sample_perc)
    assert 700000 <= size_in_bytes <= 900000
