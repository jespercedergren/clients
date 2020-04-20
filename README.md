# Clients

The purpose of the client module is to encapsulate the logic for connecting and interacting with different services, 
including cloud and storage services. 

The client classes inherits from the abstract base class and implements the abstract method _set_secrets. 
The method can be implemented using another client, for instance the SecretsManager. 
It can also be overriden when creating a patch of the client class to serve different testing purposes.
