FROM apache/airflow:2.6.0-python3.10

RUN umask 0002; \
    mkdir -p ~/opt

COPY --chown=airflow:root opt/ ~/opt/

RUN ls --recursive ~/opt/

USER airflow

RUN pip install armada-client
RUN pip install armada-airflow
