# Hadoop Unit build

To build Hadoop Unit, you need:

* jdk 1.8
* maven 3.0+
* docker

Run:

```bash
mvn install -DskipTests
```

# Built on

* [hadoop-mini-cluster-0.1.16](https://github.com/sakserv/hadoop-mini-clusters) (aka. HDP 2.6.5.0)
* [achilles-embedded-6.0.1](https://github.com/doanduyhai/Achilles) (aka. Cassandra 3.11.3)
* [testcontainer](https://www.testcontainers.org/)
* [maven resolver](https://github.com/apache/maven-resolver/)
* [embedded-elasticsearch](https://github.com/allegro/embedded-elasticsearch)
* [redis-unit](https://github.com/ishiis/redis-unit)
