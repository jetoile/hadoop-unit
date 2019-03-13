FROM java:8

WORKDIR /
ADD appassembler-jsw/jsw/hadoop-unit-standalone /
RUN chmod +x /bin/hadoop-unit-standalone && chmod +x /bin/wrapper-linux-x86-*

EXPOSE 22010  20102  20103  20112  50070  50010 25111 50075 50020 28000  20111  8983  37001  37002  37003  37004  37005  20113  14433  14533  13433  13533  8888  14001  14002  14003  14004  14005  6379  8081  22222  8082  8083

ENTRYPOINT ["bin/hadoop-unit-standalone", "console"]
