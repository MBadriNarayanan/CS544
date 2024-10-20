FROM ubuntu:22.04
RUN apt-get update; apt-get install -y wget curl openjdk-11-jdk python3-pip net-tools lsof nano

# HDFS
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz; tar -xf hadoop-3.3.6.tar.gz; rm hadoop-3.3.6.tar.gz

ENV HADOOP_HOME=/hadoop-3.3.6
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${PATH}:/hadoop-3.3.6/bin"
ENV JAVA_LIBRARY_PATH="${HADOOP_HOME}/lib/native"