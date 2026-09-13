FROM flink:2.1.0-scala_2.12-java17

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.1/flink-connector-kafka-5.0.0-2.1.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.1.1/kafka-clients-4.1.1.jar