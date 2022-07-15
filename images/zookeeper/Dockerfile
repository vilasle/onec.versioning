FROM ubuntu

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

EXPOSE 2181

RUN apt-get update && apt-getп install -y \
     openjdk-8-jre-headless wget && \
     wget -O /tmp/zookeeper.tar.gz https://dlcdn.apache.org/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz && \
     tar -xvf /tmp/zookeeper.tar.gz -C /opt && \
     mv -f /opt/apache-zookeeper-3.7.1-bin /opt/zookeeper && \
     cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg && \
     rm -rf /tmp/*

WORKDIR /opt/zookeeper

VOLUME ["/opt/zookeeper/conf", "/tmp/zookeeper"]

ENTRYPOINT ["/opt/zookeeper/bin/zkServer.sh"]

CMD ["start-foreground"]