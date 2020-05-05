import boto3
from clients.base import BaseClient
from clients.secrets_manager import SecretsManager
import numpy as np


# INSERT
python_to_dynamodb_type_map = {str: "S", int: "N", float: "N", type(b''): "B", bool: "BOOL", type(None): "NULL",
                               type([]): "L", type({}): "M"}
extra = {"StringSet": "SS", "NumberSet": "NS", "BinarySet": "BS"}


def noop(x):
    return x


def set_none_true(x):
    return True


def set_none(x):
    if x == True:
        return None


def get_list_attribute(list_attribute):
    """
    Converts a list attribute into DynamoDB format.
    :param list_attribute: [1, "value"]
    :return: [{'N': '1'}, {'S': 'value'}]
    """
    return [{python_to_dynamodb_type_map[type(value)]: python_to_dynamodb_conversion_map[type(value)](value)} for value in list_attribute]


def get_map_attribute(map_attribute):
    """
    Converts a map attribute into DynamoDB format.
    :param map_attribute: {'key_1': 'value_1', 'key_2': 2}
    :return: {'key_1': {'S', 'value_1'}, 'key_2': {'N', '2'}}
    """
    return {key: {python_to_dynamodb_type_map[type(value)]: python_to_dynamodb_conversion_map[type(value)](value)} for key, value in map_attribute.items()}


def json_to_item(item_json: dict, key_schema: dict = None):
    """
    Converts a json dictionary into a DynamoDB item.
    :param item_json: {'primary_key', 'value', 'attribute', 'attribute_value'}
    :param key_schema: schema to use for attribute {'attribute_name': <attribute_type>}
    :return: {'primary_key', {'type_primary_key': 'value'}, 'attribute', {'type_attribute_value': 'attribute_value'}}
    """
    if key_schema:
        return {key: {python_to_dynamodb_type_map[key_schema[key]]: python_to_dynamodb_conversion_map[key_schema[key]](value)}
                for key, value in item_json.items()}
    else:
        return {key: {python_to_dynamodb_type_map[type(value)]: python_to_dynamodb_conversion_map[type(value)](value)}
                for key, value in item_json.items()}

python_to_dynamodb_conversion_map = {str: noop, float: str, int: str, type(b''): noop, bool: noop,
                                     type(None): set_none_true,
                                     type([]): get_list_attribute, type({}): get_map_attribute,
                                     type(np.array([])): get_list_attribute}

# READ
def list_item_to_list(attribute_value: list):
    """
    Extracts and converts values from an attribute stored as a list in DynamboDB.
    Works for both all types of lists, i.e mixed types.
    :param attribute_value: [{'S': 'string_value_1'}, {'N': '2'}]}
    :return: ['string_value_1', 2]
    """
    return [dynamodb_to_python_conversion_map[type_key](value) for attribute_dict in attribute_value for type_key, value in attribute_dict.items()]


def map_item_to_dict(attribute_value: dict):
    """
    Extract and converts values from an attribute stored as a map in DynamoDB.
    Supports nested maps, as the item_to_json is applied recursively.

    {'nested_key_int': {'N': '1'}, 'nested_key_map': {'M': {'a': {'S': 'b'}}}} ->
    {'nested_key_int': 1.0, 'nested_key_map': {'a': 'b'}}

    :param attribute_value: {'nested_key_int': {'N': '1'}, 'nested_key_string': {'S': 'string'}}
    :return: {'nested_key_int': 1.0, 'nested_key_string': 'string'}
    """
    return item_to_json(attribute_value)


def get_python_from_dynamodb_attribute(attribute_dict: dict):
    """
    Converts value for an attribute and returns the value only.
    Applies function in corresponding lookup, dynamodb_to_python_conversion_map.
    :param attribute_dict: {'S': '1'}
    :return: 1
    """
    type_key = [*attribute_dict][0]
    attribute_value = attribute_dict[type_key]
    return dynamodb_to_python_conversion_map[type_key](attribute_value)


def item_to_json(item: dict):
    """
    Converts a DynamoDB item to a json dictionary.
    :param item: DynamoDB item {'attribute': {'attribute_type': 'value'}, ...}
    :return:
    """
    return {attribute: get_python_from_dynamodb_attribute(attribute_dict) for attribute, attribute_dict in item.items()}


# Lookup to convert store DynamoDB values to correct format
# - All numbers are stored as N (number), hence always converted to float in order to not lose information
# - Null values are store as True
dynamodb_to_python_conversion_map = {"S": str, "N": float, "B": noop, "BOOL": bool, "NULL": set_none,
                                     "L": list_item_to_list, "M": map_item_to_dict,
                                     "SS": list_item_to_list, "NS": list_item_to_list, "BS": list_item_to_list}

#from encoders.dynamodb import DynamoDBItemEncoder
#item = DynamoDBItemEncoder.from_json(item_json=item, key_schema=key_schema)
#key = DynamoDBItemEncoder.from_json(item_json=item, key_schema=key_schema)
#item_json = DynamoDBItemEncoder.from_item(item=item)


class DynamoDBClientBase(BaseClient):
    """
    Base class for an FirehoseClient that gets the secrets from AWS Secrets Manager.
    """
    def _set_secrets(self):
        """
        Gets secrets from the secrets manager as json with keys:
         - host, user, password, port
        :return:
        """
        secrets = SecretsManager().get_secrets(secret_id='dynamodb_client')
        self.secrets = secrets


class DynamoDBClient(DynamoDBClientBase):
    """
    Class that implements method put_record into an AWS Kinesis Firehose delivery stream.
    Requires delivery_stream as input when instantiated.
    """

    def __init__(self, table_name, aws_profile=None):
        super(DynamoDBClient, self).__init__(aws_profile=aws_profile)
        self.client = boto3.client("dynamodb", **self._get_secrets())
        self.table_name = table_name

    def put_item(self, item_json: dict, key_schema: dict = None):
        """
        :param item_json: dictionary of the form {<attribute_1>: <value_x>, <attribute_2>: <value_x>}
        :param key_schema:
        :return:
        """
        item = json_to_item(item_json=item_json, key_schema=key_schema)
        response = self.client.put_item(TableName=self.table_name, Item=item)
        return response

    def get_item(self, item_key: dict, key_schema: dict = None, consistent_read: bool = False):
        """
        :param item_key: dictionary of the form {<primary_key>: <value>}
        :param key_schema:
        :param consistent_read:
        :return:
        """
        key = json_to_item(item_json=item_key, key_schema=key_schema)
        item = self.client.get_item(TableName=self.table_name, Key=key, ConsistentRead=consistent_read)["Item"]
        item_json = item_to_json(item)
        return item_json


item_format = {
        'string': {
            'S': 'string',
            'N': 'string',
            'B': b'bytes',
            'SS': [
                'string',
            ],
            'NS': [
                'string',
            ],
            'BS': [
                b'bytes',
            ],
            'M': {
                'string': {'... recursive ...'}
            },
            'L': [
                {'... recursive ...'},
            ],
            'NULL': True|False,
            'BOOL': True|False
        }
}