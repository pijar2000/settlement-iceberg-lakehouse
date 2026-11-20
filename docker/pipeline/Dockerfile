# Single image: Python + Java 17 + PySpark (local mode) + Iceberg jars + DuckDB.
FROM python:3.11-slim-bookworm

ARG ICEBERG_VERSION=1.9.2
ENV PYTHONUNBUFFERED=1 SPARK_CONF_DIR=/app/config PYTHONPATH=/app

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Iceberg runtime + AWS bundle (S3FileIO) into PySpark's jars dir
RUN J=$(python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") \
 && M=https://repo1.maven.org/maven2/org/apache/iceberg \
 && curl -fsSL -o $J/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar $M/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o $J/iceberg-aws-bundle-${ICEBERG_VERSION}.jar $M/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

COPY . .
CMD ["python", "-m", "src.cli", "--help"]
