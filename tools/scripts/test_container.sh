command=$1

test_folder="$(pwd)/tests"

docker run -it -e test_env='docker' --mount type=bind,src="$test_folder",target=/tests test_base $command
