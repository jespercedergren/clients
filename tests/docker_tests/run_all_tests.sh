# spin down services
#docker-compose -f ../server/docker-compose.yml down --remove-orphans

# spin up services
docker-compose -f ../server/docker-compose.yml up -d

sleep 5

# unit tests
docker run -it -e test_env='docker' --network server_default --mount type=bind,src="$(pwd)/../../clients",target=/clients --mount type=bind,src="$(pwd)/..",target=/tests test_base python3.7 -m pytest tests/unit_tests/

# integration tests
docker run -it -e test_env='docker' --network server_default --mount type=bind,src="$(pwd)/../../clients",target=/clients --mount type=bind,src="$(pwd)/..",target=/tests test_base python3.7 -m pytest tests/integration_tests/

# spin down services
docker-compose -f ../server/docker-compose.yml down --remove-orphans
