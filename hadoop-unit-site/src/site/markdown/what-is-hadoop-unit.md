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
* and so on...

It offers different kinds of utilization:

* [a standalone mode](install-hadoop-unit-standalone.html)
* [an integration with integration tests launched with maven](maven-usage.html)

# Compatibility matrix

| Hadoop Unit version  | Hadoop mini cluster version | HDP version |
| ------------- | ------------- | ------------- |
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


# Available components

The available components are:

* HDFS
* ZOOKEEPER
* HIVEMETA
* HIVESERVER2
* SOLR
* SOLRCLOUD
* OOZIE
* KAFKA
* HBASE
* MONGODB
* CASSANDRA
* ELASTICSEARCH
* CONFLUENT (ie. schema registry, rest gateway, kafka)
* NEO4J
* KNOX
* ALLUXIO
* REDIS (for linux and macOS only)
