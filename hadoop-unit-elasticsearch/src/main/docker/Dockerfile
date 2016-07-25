FROM java:8
#FROM frolvlad/alpine-oraclejdk8

WORKDIR /
ADD appassembler-jsw/jsw/hadoop-unit-elasticsearch /
RUN chmod +x /bin/hadoop-unit-elasticsearch
RUN chmod +x /bin/wrapper-linux-x86-32
RUN chmod +x /bin/wrapper-linux-x86-64
RUN mkdir /logs

EXPOSE 14433
EXPOSE 14533

CMD /bin/hadoop-unit-elasticsearch console

