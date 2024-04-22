FROM apache/airflow:2.9.0
COPY requirements_docker.txt .
RUN pip install -r requirements_docker.txt