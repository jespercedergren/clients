import os
from abc import ABC, abstractmethod


class BaseClient(ABC):
    """
    Base class for any client connecting to a database. Need to implement the set_secrets with credentials for
    connecting to the database of interest.
    AWS profile is set at runtime if specified. This allows for patching the derived child classes from this base class
    for test purposes.
    """
    def __init__(self, aws_profile=None, secrets=None, **kwargs):

        if aws_profile:
            os.environ['AWS_PROFILE'] = aws_profile

        if secrets:
            self.secrets = secrets
        else:
            self._set_secrets(**kwargs)

    @abstractmethod
    def _set_secrets(self, **kwargs):
        """
        Needs to be implemented by a child class to set required secrets to self.secrets
        """
        pass

    def _get_secrets(self):
        return self.secrets

