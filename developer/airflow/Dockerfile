FROM apache/airflow:2.7.0-python3.10

RUN umask 0002; \
    mkdir -p /home/airflow/client
RUN umask 0002; \
    mkdir -p /home/airflow/airflow

COPY --chown=airflow:root init.sh /home/airflow/init.sh
COPY --chown=airflow:root opt/client /home/airflow/client
COPY --chown=airflow:root opt/airflow /home/airflow/airflow

USER airflow

RUN /home/airflow/init.sh
