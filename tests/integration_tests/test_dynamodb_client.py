from __future__ import print_function
import pytest
import time

from tests.patches import PatchedDynamoDBClient


#@pytest.mark.skip(reason="Skipping.")
class TestDynamoDBRead:

    @pytest.fixture(scope="session")
    def add_data(self, setup_secrets_localstack, setup_table_dynamodb):

        item_1 = {"user_id": "111", "app": "system", "permissions": "all", "other": 1}
        item_2 = {"user_id": "222", "app": "fake_app", "permissions": "basic", "other": 1}

        item_3 = {"user_id": "1000",
                  "string_attribute": "value",
                  "integer_attribute": 1,
                  "float_attribute": 1.123456789101112131415116,
                  "binary_attribute": b'binary_value',
                  "bool_attribute_true": True, "bool_attribute_false": False,
                  "null_attribute_1": None,
                  "string_set_attribute": ["string_value_1", "string_value_2"],
                  "number_set_attribute": [10, 11],
                  "binary_set_attribute": [b'1', b'value']
                  }

        item_4 = {"user_id": "2000",
                  "list_attribute": [1, "string_value_in_mixed_list"],
                  "map_attribute": {"nested_key_int": 1, "nested_key_float": 2, "nested_key_string": "string"}
                  }

        item_5 = {"user_id": "3000",
                  "map_attribute": {"nested_key_int": 1, "nested_key_map": {"a": "b"}}
                  }

        dynamodb_client = PatchedDynamoDBClient()
        dynamodb_client.put_item(item_json=item_1)
        dynamodb_client.put_item(item_json=item_2)
        dynamodb_client.put_item(item_json=item_3)
        dynamodb_client.put_item(item_json=item_4)
        dynamodb_client.put_item(item_json=item_5)

        return item_1, item_2, item_3, item_4, item_5

    def test_dynamodb_read(self, add_data):

        expected_1 = add_data[0]
        expected_2 = add_data[1]
        expected_3 = add_data[2]
        expected_4 = add_data[3]
        expected_5 = add_data[4]

        dynamodb_client = PatchedDynamoDBClient()

        start = time.perf_counter()
        response_1 = dynamodb_client.get_item({"user_id": "111"})
        response_2 = dynamodb_client.get_item({"user_id": "222"})
        response_3 = dynamodb_client.get_item({"user_id": "1000"})
        response_4 = dynamodb_client.get_item({"user_id": "2000"})
        response_5 = dynamodb_client.get_item({"user_id": "3000"})
        elapsed = time.perf_counter() - start

        print(f"Executed in {elapsed:0.2f} seconds.")

        assert expected_1 == response_1
        assert expected_2 == response_2
        assert expected_3 == response_3
        assert expected_4 == response_4
        assert expected_5 == response_5
