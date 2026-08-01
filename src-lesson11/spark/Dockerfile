FROM spark:4.0.1-scala2.13-java17-python3-ubuntu

USER root

# Deliberately minimal: the processor decodes Avro with Spark's own from_avro
# and talks to the registry over plain HTTP, so neither confluent-kafka nor
# fastavro is needed here. Adding them back would imply a second, competing
# Kafka/Avro path that this lesson does not use.
RUN python3 -m pip install --no-cache-dir \
      clickhouse-connect==0.8.18 \
      prometheus-client==0.22.1 \
      requests==2.32.4 \
    && mkdir -p /opt/lesson11/checkpoints /opt/spark/ivy \
    && chown -R spark:root /opt/lesson11 /opt/spark/ivy

COPY spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
