import os
import json
import boto3
from clients.base import BaseClient


class SecretsManagerClientBase(BaseClient):
    """
    Base class for an SecretsManagerClient that sets the secrets depending on the environment variable ENVIRONMENT.
    """
    def _set_secrets(self):
        """
        Gets secrets to access the secrets manager as json with keys:
         - endpoint_url, aws_access_key_id, aws_secret_access_key, region_name

        If ENVIRONMENT does not exists, no secrets are set and it assumes a product environment with an attached
        role.
        """

        # If ENVIRONMENT is set, use hardcoded secrets, else rely on role

        try:
            ENVIRONMENT = os.environ["ENVIRONMENT"]

            if ENVIRONMENT == "test_docker":
                secret_dict = {"endpoint_url": "http://localstack:4566",
                                 "aws_access_key_id": "test", "aws_secret_access_key": "test",
                                 "region_name": "eu-west-1"}

            elif ENVIRONMENT == "test_local":
                secret_dict = {"endpoint_url": "http://localhost:4566",
                                 "aws_access_key_id": "test", "aws_secret_access_key": "test",
                                 "region_name": "eu-west-1"}

        except KeyError as e:
            secret_dict = {}

        self.secrets = secret_dict


class SecretsManager(SecretsManagerClientBase):
    """
    Class to wrap around the AWS secrets manager that passes the necessary variables depending on the environment
    variable environment.
    """

    def __init__(self):
        super(SecretsManager, self).__init__()
        self.client = boto3.client("secretsmanager", **self._get_secrets())

    def get_secrets(self, secret_id):
        return json.loads(self.client.get_secret_value(SecretId=secret_id)['SecretString'])
