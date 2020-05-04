import boto3
import datetime
import uuid
import tempfile
import s3fs
import dask.dataframe as dd
import fastparquet

from clients.base import BaseClient
from clients.secrets_manager import SecretsManager


def get_firehose_output_s3_key(prefix="test-prefix", delivery_stream_name="api_to_s3_ingest", **kwargs):
    date_time_second = datetime.datetime.utcnow().replace(microsecond=0)
    date_slash_hour = date_time_second.strftime("%Y/%m/%d/%H")
    date_dash_second = date_time_second.strftime("%Y-%m-%d-%H-%M-%S")
    uuid_4 = uuid.uuid4().__str__()
    return f"{prefix}/{date_slash_hour}/{delivery_stream_name}-{date_dash_second}-{uuid_4}"


class S3ClientBase(BaseClient):

    def _set_secrets(self):
        """
        Gets secrets from the secrets manager as json with keys:
         - host, port, region_name, aws_access_key_id, aws_secret_access_key
        :return:
        """

        secrets = SecretsManager().get_secrets(secret_id='s3_client')
        self.secrets = secrets


class S3Client(S3ClientBase):

    def __init__(self, bucket, aws_profile=None):
        super(S3Client, self).__init__(aws_profile=aws_profile)
        self.resource = boto3.resource("s3", **self._get_secrets())
        self.bucket = bucket

    def upload_record(self, record: bytes, get_key_func, prefix=None, **kwargs):

        with tempfile.TemporaryDirectory() as temp_dir:
            key = get_key_func(**locals(), **kwargs)
            local_file = f"{temp_dir}/temp_file"

            with open(local_file, "wb") as f:
                f.write(record)

            with open(local_file, 'rb') as buffer:
                response = self.resource.Bucket(f"{self.bucket}").put_object(Key=key, Body=buffer)

        return response


class S3ParquetClient(S3ClientBase):

    def __init__(self, aws_profile=None):
        super(S3ParquetClient, self).__init__(aws_profile=aws_profile)
        self.client_kwargs = self.get_client_kwargs(self._get_secrets())
        self.fs = s3fs.core.S3FileSystem(**self.client_kwargs)

    @staticmethod
    def get_client_kwargs(secrets):
        client_kwargs = secrets.copy()

        kwargs = {}

        for client_key, secret_key in zip(["key", "secret"], ["aws_access_key_id", "aws_secret_access_key"]):
            try:
                kwargs[client_key] = client_kwargs.pop(secret_key)
            except KeyError as e:
                pass

        kwargs["client_kwargs"] = client_kwargs

        return kwargs

    def read(self, path, engine="pyarrow"):

        input_parquet_path = f"{path}/part*.parquet"
        all_paths = self.fs.glob(path=input_parquet_path)
        all_paths_s3 = ["s3://" + x for x in all_paths]

        try:
            df_dask = dd.read_parquet(path=all_paths_s3, engine=engine, storage_options=self.client_kwargs,
                                      gather_stats=False)
            df_pandas = df_dask.compute()
            return df_pandas.to_dict(orient="list")

        except Exception as e:
            raise e

    def read_fastparquet(self, path):

        s3 = s3fs.S3FileSystem(**self.client_kwargs)
        my_open = s3.open

        input_parquet_path = f"{path}/part*.parquet"
        # TODO: test with boto (or restart web app?)
        all_paths_from_s3 = self.fs.glob(path=input_parquet_path)

        pf = fastparquet.ParquetFile(all_paths_from_s3, open_with=my_open)
        pf_pd = pf.to_pandas()

        return pf_pd.to_dict(orient="list")


class DaskS3(S3ClientBase):

    def __init__(self, aws_profile=None):
        super(DaskS3, self).__init__(aws_profile=aws_profile)
        self.client_kwargs = self.get_client_kwargs(self._get_secrets())
        self.fs = s3fs.core.S3FileSystem(**self.client_kwargs)

    @staticmethod
    def get_client_kwargs(secrets):
        client_kwargs = secrets.copy()

        kwargs = {}

        for client_key, secret_key in zip(["key", "secret"], ["aws_access_key_id", "aws_secret_access_key"]):
            try:
                kwargs[client_key] = client_kwargs.pop(secret_key)
            except KeyError as e:
                pass

        kwargs["client_kwargs"] = client_kwargs

        return kwargs

    def read_parquet(self, path, engine="pyarrow"):

        input_parquet_path = f"{path}/part*.parquet"
        all_paths = self.fs.glob(path=input_parquet_path)
        all_paths_s3 = ["s3://" + x for x in all_paths]

        df_dask = dd.read_parquet(path=all_paths_s3, engine=engine, storage_options=self.client_kwargs,
                                  gather_stats=False)
        return df_dask

    def read_json(self, path):
        all_paths = self.fs.glob(path=path)
        all_paths_s3 = ["s3://" + x for x in all_paths]

        return dd.read_json(url_path=all_paths_s3, storage_options=self.client_kwargs)

    def save_parquet(self, df_dask, outfile, engine="pyarrow"):
        dd.to_parquet(df_dask, path=outfile, engine=engine, storage_options=self.client_kwargs)
