# download the needful
pip3 install "./third_party/airflow/" "kubernetes"

# Airflow needs a home. `~/airflow` is the default, but you can put it
# somewhere else if you prefer (optional)
export AIRFLOW_HOME=~/airflow
mkdir -p ~/airflow/dags
cp ./third_party/airflow/examples/* ~/airflow/dags
sed -i 's/127.0.0.1:50051/server:50051/g' ~/airflow/dags/*.py
sed -i 's/127.0.0.1:60003/jobservice:60003/g' ~/airflow/dags/*.py
sed -i 's/127.0.0.1:8089/lookout:8089/g' ~/airflow/dags/*.py
sed -i 's/test/queue-a/g' ~/airflow/dags/*.py

# Run this command in any terminal.
airflow db init
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

# Run the webserver on port 8081 as a background job
# 8081 is because Armada already has something listening on 8080 (default airflow port)
airflow webserver -p 8081 &

# Run the scheduler
airflow scheduler

