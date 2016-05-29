FROM java:8

WORKDIR /
ADD appassembler-jsw/jsw/hadoop-unit-hive /
RUN chmod +x /bin/hadoop-unit-hive
RUN chmod +x /bin/wrapper-linux-x86-32
RUN chmod +x /bin/wrapper-linux-x86-64
RUN mkdir /logs

EXPOSE 20102
EXPOSE 20103

CMD /bin/hadoop-unit-hive console

