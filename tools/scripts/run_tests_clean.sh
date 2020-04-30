test_arg=$1

#spin down services
./tools/scripts/spin_down.sh

# spin up services
./tools/scripts/spin_up.sh

test_folder="$(pwd)/tests"

if [ "$test_arg" == "" ] ; then
  echo "Running unit and integration tests."
  # unit tests
  docker run -it -e test_env='docker' --network server_default --mount type=bind,src="$clients_folder",target=/clients --mount type=bind,src="$encoders_folder",target=/encoders --mount type=bind,src="$test_folder",target=/tests test_base python3.7 -m pytest tests/unit_tests/ -s
  # integration tests
  docker run -it -e test_env='docker' --network server_default --mount type=bind,src="$clients_folder",target=/clients --mount type=bind,src="$encoders_folder",target=/encoders --mount type=bind,src="$test_folder",target=/tests test_base python3.7 -m pytest tests/integration_tests/ -s
else
  echo "Running $test_arg."
  docker run -it -e test_env='docker' --network server_default --mount type=bind,src="$clients_folder",target=/clients --mount type=bind,src="$encoders_folder",target=/encoders --mount type=bind,src="$test_folder",target=/tests test_base python3.7 -m pytest "tests/$test_arg" -s
fi

#spin down services
./tools/scripts/spin_down.sh
