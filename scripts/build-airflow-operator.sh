#!/bin/bash
# This script is intended to be run under the docker container at $ARMADADIR/build/python-api-client/

# make the python package armada.client, not pkg.api
mkdir -p proto-airflow
cp pkg/api/jobservice/jobservice.proto proto-airflow
sed -i 's/\([^\/]\)pkg\/api/\1jobservice/g' proto-airflow/*.proto


# generate python stubs
cd proto-airflow 
python3 -m grpc_tools.protoc -I. --plugin=protoc-gen-mypy=$(which protoc-gen-mypy) --mypy_out=../third_party/airflow/armada/jobservice --python_out=../third_party/airflow/armada/jobservice --grpc_python_out=../third_party/airflow/armada/jobservice \
    jobservice.proto 
cd ..
# This hideous code is because we can't use python package option in grpc.
# See https://github.com/protocolbuffers/protobuf/issues/7061 for an explanation.
# We need to import these packages as a module.
sed -i 's/import jobservice_pb2 as jobservice__pb2/from armada.jobservice import jobservice_pb2 as jobservice__pb2/g' third_party/airflow/armada/jobservice/*.py
