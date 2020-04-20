from __future__ import print_function
import pytest
import json
from tests.helpers.common import get_all_s3_keys, read_s3_stream


def json_to_record_payload(data_json):
    data_string = json.dumps(data_json)
    data_bytes = data_string.encode("utf-8")
    return data_bytes


#@pytest.mark.skip(reason="Skipping.")
class TestBasic:

    @pytest.fixture()
    def add_setup(self, setup_s3_bucket_localstack, setup_firehose_delivery_stream_localstack):
        """
        :param spark_session:
        :return:
        """
        pass

    def test_firehose(self, add_setup):

        from tests.patches import PatchedFirehoseClient
        firehose_client = PatchedFirehoseClient()

        i = 0
        input_json = {f"some_key_{i}": f"some_value_{i}"}
        record_payload = json_to_record_payload(data_json=input_json)

        response = firehose_client.put_record(record=record_payload)

        bucket_keys = get_all_s3_keys()

        for bucket, keys in bucket_keys.items():
            for key in keys:
                if "test-prefix" in key:
                    data_json = read_s3_stream(bucket, key)

        assert input_json == data_json
