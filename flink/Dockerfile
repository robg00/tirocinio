FROM flink:1.20

# Installa Python 3 e pip
RUN apt-get update && apt-get install -y python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

# Installa PyFlink (match versione Flink)
RUN pip3 install apache-flink==1.20.0

# Abilita il JAR Python di Flink
RUN cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/

# Connector JARs (Kafka + JDBC + PostgreSQL)
RUN wget -nv -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.5.jar
