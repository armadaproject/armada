#!/bin/bash
# This script is intended to be run under the docker container at $ARMADADIR/build/python-api-client/

# make the python package armada.client, not pkg.api
mkdir -p proto-airflow/armada
cp pkg/api/jobservice/jobservice.proto proto-airflow/armada
sed -i 's/\([^\/]\)pkg\/api/\1armada/g' proto-airflow/armada/*.proto


# generate python stubs
cd proto 
python3 -m grpc_tools.protoc -I. --python_out=../third_party/airflow/jobservice --grpc_python_out=../third_party/airflow/jobservice \
    armada/jobservice.proto 
cd ..
# This hideous code is because we can't use python package option in grpc.
# See https://github.com/protocolbuffers/protobuf/issues/7061 for an explanation.
# We need to import these packages as a module.
sed -i 's/from armada/from armada_client.armada/g' third_party/airflow/jobservice/*.py
