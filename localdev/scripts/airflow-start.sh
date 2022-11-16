# download the needful
pip3 install "./third_party/airflow/" "kubernetes"

# Airflow needs a home. `~/airflow` is the default, but you can put it
# somewhere else if you prefer (optional)
export AIRFLOW_HOME=~/airflow
mkdir -p ~/airflow/dags
cp ./third_party/airflow/examples/* ~/airflow/dags

# Run this command in any terminal.
airflow db init
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

# Run the webserver on port 8081 as a background job
# 8081 is because Armada already has something listening on 8080 (default airflow port)
airflow webserver -p 8081 &

# Run the scheduler
airflow scheduler

