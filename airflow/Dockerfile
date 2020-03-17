FROM puckel/docker-airflow

USER root

COPY requirements.txt .

RUN apt-get update && \
echo Y | apt install python3-pip && \
pip install -U pip && \
pip install -r requirements.txt && \
apt-get install cifs-utils -y
