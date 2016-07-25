FROM java:8
#FROM frolvlad/alpine-oraclejdk8

WORKDIR /
ADD appassembler-jsw/jsw/hadoop-unit-zookeeper /
RUN chmod +x /bin/hadoop-unit-zookeeper
RUN chmod +x /bin/wrapper-linux-x86-32
RUN chmod +x /bin/wrapper-linux-x86-64
RUN mkdir /logs

EXPOSE 22010

CMD /bin/hadoop-unit-zookeeper console

