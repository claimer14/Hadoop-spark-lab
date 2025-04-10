FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    && pip3 install psutil matplotlib pandas

COPY . /app
WORKDIR /app

CMD ["/opt/spark/bin/spark-submit", "--master", "spark://spark:7077", "spark_analysis.py"] 