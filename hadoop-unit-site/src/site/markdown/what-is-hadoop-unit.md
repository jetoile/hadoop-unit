# Purpose

The main purpose of Hadoop Unit is to allow users to test the integration of their developments in the hadoop world.

Indeed, it is common to need the hadoop's ecosystem to make integration test like:

* hdfs
* hbase
* hive (hiveserver and hivemetastore)
* elasticsearch
* solr/solrcloud
* mongodb
* cassandra
* kafka
* kafka confluent
* and so on...

Since v3.2, Hadoop Unit supports docker and docker-compose to allow to run anything.

It offers different kinds of utilization:

* [a standalone mode](install-hadoop-unit-standalone.html)
* [an integration with integration tests launched with maven - version 2.x](maven-usage_2.x.html)
* [an integration with integration tests launched with maven - version 3.x](maven-usage_3.x.html)

# Compatibility matrix

| Hadoop Unit version  | Hadoop mini cluster version | HDP version |
| ------------- | ------------- | ------------- |
| 3.5 | 0.1.16 | HDP 2.6.5.0 |
| 3.4 | 0.1.16 | HDP 2.6.5.0 |
| 3.3 | 0.1.16 | HDP 2.6.5.0 |
| 3.2 | 0.1.16 | HDP 2.6.5.0 |
| 3.1 | 0.1.16 | HDP 2.6.5.0 |
| 2.10.1 | 0.1.16 | HDP 2.6.5.0 |
| 2.9 | 0.1.15 | HDP 2.6.3.0 |
| 2.8 | 0.1.14 | HDP 2.6.2.0 |
| 2.7 | 0.1.14 | HDP 2.6.2.0 |
| 2.6 | 0.1.14 | HDP 2.6.2.0 |
| 2.5 | 0.1.14 | HDP 2.6.2.0 |
| 2.4 | 0.1.14 | HDP 2.6.2.0 |
| 2.3 | 0.1.14 | HDP 2.6.2.0 |
| 2.2 | 0.1.12 | HDP 2.6.1.0 |
| 2.1 | 0.1.11 | HDP 2.5.3.0 |
| 2.0 | 0.1.9 | HDP 2.5.3.0 |
| 1.5 | 0.1.8 | HDP 2.5.0.0 |
| 1.4 | 0.1.7 | HDP 2.4.2.0 |
| 1.3 | 0.1.6 | HDP 2.4.0.0 |
| 1.2 | 0.1.5 | HDP 2.4.0.0 |


__Warning__: Major releases are breaking compatibility

# Available components

The available components are:

* HDFS
* HDFS 3
* ZOOKEEPER
* HIVEMETA
* HIVESERVER2
* HIVEMETASTORE 3
* HIVESERVER2 3
* YARN
* YARN 3
* SOLR
* SOLRCLOUD
* OOZIE
* KAFKA
* CONFLUENT KAFKA
* CONFLUENT KAFKA SCHEMA REGISTRY
* CONFLUENT KAFKA REST GATEWAY
* CONFLUENT KAFKA KSQL
* HBASE
* MONGODB
* CASSANDRA
* ELASTICSEARCH
* NEO4J
* KNOX
* ALLUXIO
* BOOKKEEPER
* PULSAR
* PULSAR_STANDALONE
* REDIS (for linux and macOS only)

