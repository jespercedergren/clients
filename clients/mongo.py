from pymongo import MongoClient
from clients.base import BaseClient
from clients.secrets_manager import SecretsManager


def get_client_arg_from_secrets(secrets):
    """
    Creates a string that can be passed to passed to pymongo.MongoClient.
    :param secrets: Json with keys: host, port, user, password
    :return: clent_arg: string
    """
    client_arg = f"mongodb://{secrets['user']}:{secrets['password']}@{secrets['host']}:{secrets['port']}"
    return client_arg


class MongoDBClientBase(BaseClient):
    """
    Base class for an MongoClient that gets the secrets from AWS Secrets Manager.
    """
    def _set_secrets(self):
        """
        Gets secrets from the secrets manager as json with keys:
         - host, user, password, port
        :return:
        """
        secrets = SecretsManager().get_secrets(secret_id='mongo_client')
        self.secrets = get_client_arg_from_secrets(secrets)


class MongoDBConnectionManager:
    """
    Context manager for pymongo.MongoClient.
    """
    def __init__(self, client_arg):
        self.client_arg = client_arg
        self.connection = None

    def __enter__(self):
        self.connection = MongoClient(self.client_arg)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.close()


class MongoDBClient(MongoDBClientBase):
    """
    Class that implements methods the find and save on MongoDB database collections using the ContextManager MongoDBConnectionManager.
    Requires database and collection as input when instantiated.
    """
    def __init__(self, database, collection):
        super(MongoDBClient, self).__init__()
        self.database = database
        self.collection = collection

    def find(self, query):
        with MongoDBConnectionManager(self._get_secrets()) as mongo:
            collection = mongo.connection[self.database][self.collection]
            response = collection.find(query)
            return [resp for resp in response]

    def save(self, query):
        with MongoDBConnectionManager(self._get_secrets()) as mongo:
            collection = mongo.connection[self.database][self.collection]
            response = collection.save(query)
            return response

