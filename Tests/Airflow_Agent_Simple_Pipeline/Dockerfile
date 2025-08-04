FROM apache/airflow:2.7.1

USER root
RUN apt-get update && apt-get install -y git

WORKDIR /opt/airflow
COPY test_dag.sh /opt/airflow/test_dag.sh
RUN chmod +x /opt/airflow/test_dag.sh

# Switch back to airflow user after setting permissions
USER airflow

ENTRYPOINT ["/opt/airflow/test_dag.sh"] 