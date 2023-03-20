# Airflow needs a home. `~/airflow` is the default, but you can put it
# somewhere else if you prefer (optional)
export AIRFLOW_HOME=~/airflow

# Run this command in any terminal.
python -m airflow users create --role Admin -u admin -e admin -f admin -l admin --password admin
airflow db init
# Run this command in a separate terminal.
# 8081 is because Armada already has something listening on 8080 (default airflow port)
airflow webserver -p 8081
# Run this command in a separate terminal
airflow scheduler
