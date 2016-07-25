FROM java:8
#FROM frolvlad/alpine-oraclejdk8

WORKDIR /
ADD appassembler-jsw/jsw/hadoop-unit-hbase /
RUN chmod +x /bin/hadoop-unit-hbase
RUN chmod +x /bin/wrapper-linux-x86-32
RUN chmod +x /bin/wrapper-linux-x86-64
RUN mkdir /logs

EXPOSE 22010

CMD /bin/hadoop-unit-hbase console

