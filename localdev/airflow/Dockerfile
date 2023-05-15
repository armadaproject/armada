FROM apache/airflow:2.6.0-python3.10

RUN umask 0002; \
    mkdir -p /home/airflow/client
RUN umask 0002; \
    mkdir -p /home/airflow/airflow

COPY --chown=airflow:root opt/client /home/airflow/client
COPY --chown=airflow:root opt/airflow /home/airflow/airflow

USER airflow

RUN pip install -e /home/airflow/client
RUN pip install -e /home/airflow/airflow
