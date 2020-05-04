from __future__ import print_function
import pytest
from encoders.dynamodb import DynamoDBItemEncoder


def encode_item_json_back_and_forth(item_json):
    item = DynamoDBItemEncoder.from_json(item_json=item_json)
    item_json = DynamoDBItemEncoder.from_item(item=item)
    return item_json


#@pytest.mark.skip(reason="Skipping.")
class TestDynamoDBEncoder:

    @pytest.fixture(scope="session")
    def add_data(self):

        #item_1 = {"user_id": "111", "app": "system", "permissions": "all", "other": 1}
        #item_2 = {"user_id": "222", "app": "fake_app", "permissions": "basic", "other": 1}

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

        return item_3, item_4, item_5

    def test_dynamodb_encoder_back_and_forth(self, add_data):

        for item_json in add_data:
            assert item_json == encode_item_json_back_and_forth(item_json.copy())
