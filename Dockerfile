FROM apache/airflow:2.1.2-python3.8

ARG AIRFLOW_VERSION=2.1.2
ARG PYTHON_VERSION=3.8

ARG AIRFLOW_DEPS="google"
ARG PYTHON_DEPS=""

RUN pip install apache-airflow[kubernetes,snowflake${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi

COPY script/entrypoint.sh /entrypoint.sh
COPY config/* $AIRFLOW_HOME/
COPY dags $AIRFLOW_HOME/dags
COPY dags/creds/* /

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENTRYPOINT ["/entrypoint.sh"]