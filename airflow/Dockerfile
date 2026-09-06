FROM apache/airflow:3.3.1

USER airflow

RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-bigquery==1.12.0 \
    && /opt/airflow/dbt-venv/bin/python -m dbt.cli.main --version

ENV PATH="/opt/airflow/dbt-venv/bin:${PATH}"