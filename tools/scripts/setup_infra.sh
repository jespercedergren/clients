echo "setting up infra"
docker run -it -e test_env='docker' --network server_default --mount type=bind,src="$clients_folder",target=/clients  --mount type=bind,src="$(pwd)/tests",target=/tests --mount type=bind,src="$(pwd)/tests/setup/setup_infra.py",target=/setup_infra.py test_base python3.7 /setup_infra.py
