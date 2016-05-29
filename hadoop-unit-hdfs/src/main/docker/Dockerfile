FROM java:8

WORKDIR /
ADD appassembler /
RUN chmod +x /bin/main.sh

EXPOSE 20112
EXPOSE 50070

CMD /bin/main.sh

