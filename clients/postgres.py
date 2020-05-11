import psycopg2
from clients.base import BaseClient
from clients.secrets_manager import SecretsManager


class PostgresClientBase(BaseClient):
    """
    Base class for a Postgres database that gets the secrets from AWS Secrets Manager.
    """
    def _set_secrets(self, **kwargs):
        """
        Gets secrets from the secrets manager as json with keys:
         - host, dbname, user, password, port
        :return:
        """
        secrets = SecretsManager().get_secrets(secret_id='postgres_client')

        self.secrets = secrets


class PostgresClient(PostgresClientBase):
    """
    Class that implements the basic methods query and insert using a ContextManager.
    """
    def _query(self, query, *args):
        """
        :param query: SQL query to execute (read-only).
        :return: Returns the response of the SQL query.
        """
        with psycopg2.connect(**self._get_secrets()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, *args)
                return cur.fetchall()

    def _insert(self, query, *args):
        """
        Method for altering, commits the changes.
        :param query: SQL query to execute.
        :return: Returns True if successful.
        """
        with psycopg2.connect(**self._get_secrets()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, *args)
                return True

    def _transaction(self, queries):
        """
        Method for sending multiple _insert-queries, connecting only once.
        :param queries:
        :return:
        """

        with psycopg2.connect(**self._get_secrets()) as conn:
            with conn.cursor() as cur:
                for query in queries:
                    try:
                        cur.execute(query)
                    except psycopg2.errors.SyntaxError as e:
                        print(query)
                        raise e

                return True
