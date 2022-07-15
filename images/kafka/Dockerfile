FROM ubuntu

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

EXPOSE 9092

RUN apt-get update && apt-get install -y \
     openjdk-8-jre-headless net-tools telnet iputils-ping wget && \
     wget -O /tmp/kafka.tgz https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz && \
     tar -xvf /tmp/kafka.tgz -C /opt && \
     mv -f /opt/kaf\ka_2.13-3.0.0 /opt/kafka && \
     rm -rf /tmp

WORKDIR /opt/kafka

VOLUME ["/opt/kafka/config", "/tmp/kafka-logs"]

ENTRYPOINT ["/opt/kafka/bin/kafka-server-start.sh"]

CMD [ "/opt/kafka/config/server.properties"]
