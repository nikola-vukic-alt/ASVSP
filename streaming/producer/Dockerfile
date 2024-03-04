FROM python:3.7-slim-buster

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

RUN java -version

RUN pip install --no-cache-dir  pyspark==3.0.1 kafka-python

WORKDIR /usr/src/app

COPY . .

ENV KAFKA_BROKER="localhost:9092"
ENV KAFKA_RECONNECT_BACKOFF_MS=100

CMD ["python", "-u", "./main.py"]