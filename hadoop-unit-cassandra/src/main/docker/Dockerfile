FROM java:8
#FROM frolvlad/alpine-oraclejdk8

WORKDIR /
ADD appassembler-jsw/jsw/hadoop-unit-cassandra /
RUN chmod +x /bin/hadoop-unit-cassandra
RUN chmod +x /bin/wrapper-linux-x86-32
RUN chmod +x /bin/wrapper-linux-x86-64
RUN mkdir /logs

EXPOSE 13433

CMD /bin/hadoop-unit-cassandra console

