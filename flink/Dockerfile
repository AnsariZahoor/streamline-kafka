FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

RUN pip install --no-cache-dir apache-flink

RUN mkdir -p /app/jars && \
    curl -fSL -o /app/jars/flink-sql-connector-kafka.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"

COPY spread_analytics.py .

CMD ["python", "-u", "spread_analytics.py"]
