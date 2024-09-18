FROM apache/airflow:2.10.1

USER airflow 

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER airflow