from __future__ import print_function
import pytest
import json
from tests.patches import PatchedS3Client
from clients.s3 import get_firehose_output_s3_key
from tests.helpers.common import get_all_s3_keys, read_s3_stream


def json_to_record(data_json):
    data_string = json.dumps(data_json)
    data_bytes = data_string.encode("utf-8")
    return data_bytes


#@pytest.mark.skip(reason="Skipping.")
class TestBasic:

    @pytest.fixture()
    def add_setup(self, setup_s3_bucket_minio, request):
        pass

    def test_s3_client(self, add_setup):

        s3_client = PatchedS3Client()

        i = 0
        input_json = {f"some_key_{i}": f"some_value_{i}"}
        record = json_to_record(data_json=input_json)

        response = s3_client.upload_record(record=record, get_key_func=get_firehose_output_s3_key, prefix="test-prefix")

        bucket_keys = get_all_s3_keys(s3_host="minio")

        for bucket, keys in bucket_keys.items():
            for key in keys:
                if "test-prefix" in key:
                    data_json = read_s3_stream(bucket, key, s3_host="minio")

        assert input_json == data_json
