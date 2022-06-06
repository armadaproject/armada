poetry run python3 unit/grpc_server_mock/server_mock.py &
poetry run pytest -v unit/test_client.py
kill $!
