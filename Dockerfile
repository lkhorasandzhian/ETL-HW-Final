FROM apache/airflow:2.8.1

COPY requirements.txt /requirements.txt

RUN python -m pip install --no-cache-dir -r /requirements.txt