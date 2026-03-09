FROM apache/airflow:2.11.1

COPY requirements.txt /requirements.txt

RUN python -m pip install --no-cache-dir -r /requirements.txt