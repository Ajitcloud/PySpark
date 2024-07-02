FROM spark-base/spark-py:1.0.1

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY kafka.py .
