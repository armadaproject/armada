import armada.operators.grpc


def test_serialize_grpc_channel():
    src_chan_args = {
        "target": "localhost:443",
        "credentials_callback_args": {
            "module_name": "channel_test",
            "function_name": "get_credentials",
            "function_kwargs": {
                "example_arg": "test",
            },
        },
    }

    source = armada.operators.grpc.GrpcChannelArguments(**src_chan_args)

    serialized = source.serialize()
    assert serialized["target"] == src_chan_args["target"]
    assert (
        serialized["credentials_callback_args"]
        == src_chan_args["credentials_callback_args"]
    )

    reconstituted = armada.operators.grpc.GrpcChannelArguments(**serialized)
    assert reconstituted == source
