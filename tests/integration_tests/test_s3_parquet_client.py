from __future__ import print_function
import pytest
import pandas as pd
import time

from tests.patches import PatchedS3ParquetClient
from tests.helpers.io import save_parquet
from tests.helpers.common import pd_equals


#@pytest.mark.skip(reason="Skipping.")
class TestS3ParquetRead:

    @pytest.fixture(scope="session")
    def add_data(self, setup_secrets_localstack, setup_s3_bucket_minio, spark_session_minio):
        spark = spark_session_minio
        start = time.perf_counter()

        input_data_pd = pd.DataFrame([["system", "all", "1"], ["fake_app", "basic", "1"]],
                                     columns=["app", "permissions", "other"])
        input_data_df = spark.createDataFrame(input_data_pd)

        id_key = 1
        s3_path = f"s3://test-bucket/aggregated/mobile_data.parquet/id_key={id_key}"
        save_parquet(input_data_df, s3_path)

        return input_data_pd, s3_path

    def test_s3_client_dask_fastparquet(self, add_data):

        expected_data = add_data[0]
        s3_path = add_data[1]

        s3_parquet_client = PatchedS3ParquetClient()

        start = time.perf_counter()
        response = s3_parquet_client.read(path=s3_path, engine="fastparquet")
        response_pd = pd.DataFrame(response)

        elapsed = time.perf_counter() - start
        print(f"Executed in {elapsed:0.2f} seconds.")

        assert pd_equals(expected_data, response_pd)

    def test_s3_client_dask_pyarrow(self, add_data):

        expected_data = add_data[0]
        s3_path = add_data[1]

        s3_parquet_client = PatchedS3ParquetClient()

        start = time.perf_counter()
        response = s3_parquet_client.read(path=s3_path, engine="pyarrow")
        response_pd = pd.DataFrame(response)

        elapsed = time.perf_counter() - start
        print(f"Executed in {elapsed:0.2f} seconds.")

        assert pd_equals(expected_data, response_pd)
