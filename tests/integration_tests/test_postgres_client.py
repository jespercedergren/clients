from __future__ import print_function
import pytest
import time

from tests.patches import PatchedPostgresClient

#@pytest.mark.skip(reason="Skipping.")
class TestPostgresRead:

    @pytest.fixture(scope="function")
    def add_data(self, clean_postgres_database):

        item_1 = None
        item_2 = None
        postgres_client = PatchedPostgresClient()
        #market_client = MarketPostgresClient()
        import json
        item_supply_1 = json.loads('{"user_id": 1, "service": "fixing", "price": 100}')
        item_supply_2 = json.loads('{"user_id": 2, "service": "fixing", "price": 200}')
        item_supply_3 = json.loads('{"user_id": 3, "service": "fixing", "price": 201}')

        item_demand = json.loads('{"user_id": 101, "service": "fixing", "price": 200}')

        postgres_client.insert_item(item=item_supply_1, table_name='supply_raw')
        postgres_client.insert_item(item=item_supply_2, table_name='supply_raw')
        postgres_client.insert_item(item=item_supply_3, table_name='supply_raw')

        postgres_client.insert_item(item=item_demand, table_name='demand_raw')

        response_market = postgres_client.read_market()
        response_market_raw = postgres_client.read_market_raw()

        return item_1, item_2

    def test_postgres_read(self, add_data):

        expected_1 = add_data[0]
        expected_2 = add_data[1]
        expected_3 = []

        postgres_client = PatchedPostgresClient()

        start = time.perf_counter()
        response_1 = postgres_client._query("SELECT * FROM supply")
        response_2 = postgres_client._query("SELECT * FROM demand")
        response_3 = postgres_client._query("SELECT * FROM match_table")
        elapsed = time.perf_counter() - start

        print(f"Executed in {elapsed:0.2f} seconds.")

        assert expected_1 == response_1
        assert expected_2 == response_2
        assert expected_3 == response_3
