def noop(x):
    return x


def set_none_true(x):
    return True


def set_none(x):
    if x == True:
        return None


class DynamoDBItemEncoder(object):

    extra = {"StringSet": "SS", "NumberSet": "NS", "BinarySet": "BS"}

    def __init__(self, item_json: dict = None, item: dict = None, key_schema: dict = None, *args, **kwargs):
        if item_json:
            self._item = self.json_to_item(item_json=item_json, key_schema=key_schema, *args, **kwargs)
        elif item:
            self._item_json = self.item_to_json(item=item, *args, **kwargs)
        else:
            raise Exception

    @property
    def item(self):
        return self._item

    @property
    def item_json(self):
        return self._item_json

    @classmethod
    def from_json(cls, item_json: dict = None, key_schema: dict = None, *args, **kwargs):
        cls.python_to_dynamodb_type_map = {str: "S", int: "N", float: "N", type(b''): "B", bool: "BOOL",
                                            type(None): "NULL",
                                            type([]): "L", type({}): "M"}

        cls.python_to_dynamodb_conversion_map = {str: noop, float: str, int: str, type(b''): noop, bool: noop,
                                                  type(None): set_none_true,
                                                  type([]): cls.get_list_attribute,
                                                  type({}): cls.get_map_attribute}
        return cls(item_json=item_json, key_schema=key_schema, *args, **kwargs).item

    @classmethod
    def from_item(cls, item: dict, *args, **kwargs):
        # Lookup to convert store DynamoDB values to correct format
        # - All numbers are stored as N (number), hence always converted to float in order to not lose information
        # - Null values are store as True

        cls.dynamodb_to_python_conversion_map = {"S": str, "N": float, "B": noop, "BOOL": bool, "NULL": set_none,
                                                  "L": cls.list_item_to_list, "M": cls.map_item_to_dict,
                                                  "SS": cls.list_item_to_list, "NS": cls.list_item_to_list,
                                                  "BS": cls.list_item_to_list}

        return cls(item=item, *args, **kwargs).item_json

    def get_list_attribute(self, list_attribute):
        """
        Converts a list attribute into DynamoDB format.
        :param list_attribute: [1, "value"]
        :return: [{'N': '1'}, {'S': 'value'}]
        """
        return [{self.python_to_dynamodb_type_map[type(value)]: self.python_to_dynamodb_conversion_map[type(value)](value)} for
                value in list_attribute]

    def get_map_attribute(self, map_attribute):
        """
        Converts a map attribute into DynamoDB format.
        :param map_attribute: {'key_1': 'value_1', 'key_2': 2}
        :return: {'key_1': {'S', 'value_1'}, 'key_2': {'N', '2'}}
        """
        return {key: {self.python_to_dynamodb_type_map[type(value)]: self.python_to_dynamodb_conversion_map[type(value)](value)}
                for key, value in map_attribute.items()}

    def json_to_item(self, item_json: dict, key_schema: dict = None):
        """
        Converts a json dictionary into a DynamoDB item.
        :param item_json: {'primary_key', 'value', 'attribute', 'attribute_value'}
        :param key_schema: schema to use for attribute {'attribute_name': <attribute_type>}
        :return: {'primary_key', {'type_primary_key': 'value'}, 'attribute', {'type_attribute_value': 'attribute_value'}}
        """
        if key_schema:
            return {key: {
                self.python_to_dynamodb_type_map[key_schema[key]]: self.python_to_dynamodb_conversion_map[key_schema[key]](value)}
                    for key, value in item_json.items()}
        else:
            return {
                key: {self.python_to_dynamodb_type_map[type(value)]: self.python_to_dynamodb_conversion_map[type(value)](value)}
                for key, value in item_json.items()}

    def list_item_to_list(self, attribute_value: list):
        """
        Extracts and converts values from an attribute stored as a list in DynamboDB.
        Works for both all types of lists, i.e mixed types.
        :param attribute_value: [{'S': 'string_value_1'}, {'N': '2'}]}
        :return: ['string_value_1', 2]
        """
        return [self.dynamodb_to_python_conversion_map[type_key](value) for attribute_dict in attribute_value for
                type_key, value in attribute_dict.items()]

    def map_item_to_dict(self, attribute_value: dict):
        """
        Extract and converts values from an attribute stored as a map in DynamoDB.
        Supports nested maps, as the item_to_json is applied recursively.

        {'nested_key_int': {'N': '1'}, 'nested_key_map': {'M': {'a': {'S': 'b'}}}} ->
        {'nested_key_int': 1.0, 'nested_key_map': {'a': 'b'}}

        :param attribute_value: {'nested_key_int': {'N': '1'}, 'nested_key_string': {'S': 'string'}}
        :return: {'nested_key_int': 1.0, 'nested_key_string': 'string'}
        """
        return self.item_to_json(attribute_value)

    def get_python_from_dynamodb_attribute(self, attribute_dict: dict):
        """
        Converts value for an attribute and returns the value only.
        Applies function in corresponding lookup, dynamodb_to_python_conversion_map.
        :param attribute_dict: {'S': '1'}
        :return: 1
        """
        type_key = [*attribute_dict][0]
        attribute_value = attribute_dict[type_key]
        return self.dynamodb_to_python_conversion_map[type_key](attribute_value)

    def item_to_json(self, item: dict):
        """
        Converts a DynamoDB item to a json dictionary.
        :param item: DynamoDB item {'attribute': {'attribute_type': 'value'}, ...}
        :return:
        """
        return {attribute: self.get_python_from_dynamodb_attribute(attribute_dict) for attribute, attribute_dict in
                item.items()}


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


if __name__ == "__main__":
    item = DynamoDBItemEncoder.from_json(item_json=item_1)
    print(item)
    item_json = DynamoDBItemEncoder.from_item(item=item)
    print(item_json)
    print(item_json == item_1)