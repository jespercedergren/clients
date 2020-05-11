import boto3
from clients.base import BaseClient
from clients.secrets_manager import SecretsManager


class FirehoseClientBase(BaseClient):
    """
    Base class for an FirehoseClient that gets the secrets from AWS Secrets Manager.
    """
    def _set_secrets(self, **kwargs):
        """
        Gets secrets from the secrets manager as json with keys:
         - host, user, password, port
        :return:
        """
        secrets = SecretsManager().get_secrets(secret_id='firehose_client')
        self.secrets = secrets


class FirehoseClient(FirehoseClientBase):
    """
    Class that implements method put_record into an AWS Kinesis Firehose delivery stream.
    Requires delivery_stream as input when instantiated.
    """

    def __init__(self, delivery_stream_name, aws_profile=None, secrets=None):
        super(FirehoseClient, self).__init__(aws_profile=aws_profile, secrets=secrets)
        self.delivery_stream_name = delivery_stream_name
        self.client = boto3.client("firehose", **self._get_secrets())

    def put_record(self, record: bytes):
        record_payload = {'Data': record}
        response = self.client.put_record(DeliveryStreamName=self.delivery_stream_name, Record=record_payload)
        return response
