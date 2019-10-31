# Focus

* [Focus on Elasticsearch](#focus-on-elasticsearch)
* [Focus on Redis](#focus-on-redis)
* [Focus on Oozie](#focus-on-oozie)
* [Focus on Hive 3](#focus-on-hive3)
* [Focus on Hadodp 3](#focus-on-hadoop3)
* [Focus on Pulsar](#focus-on-pulsar)


<div id="focus-on-elasticsearch"/>
# Focus on ElasticSearch

Under the hood, to run ElasticSearch, this is how it works:

* ElasticSearch is downloaded
* A Java Process is run to start ElasticSearch

To know from where ElasticSearch has to be downloaded, the variable ```elasticsearch.download.url``` from the configuration file ```hadoop-unit-default.properties``` is used.

To know which version of ElasticSearch has to be downloaded, the variable ```elasticsearch.version``` from the configuration file ```hadoop-unit-default.properties``` is used.

<div id="focus-on-redis"/>
# Focus on Redis

Under the hood, to run Redis, this is how it works:

* Redis is downloaded
* An ant task is run to execute the ```make``` command

This is why Redis is not available on Windows.

To know where Redis has to be downloaded, the variable ```redis.download.url``` from the configuration file ```hadoop-unit-default.properties``` is used.

To know which version of Redis has to be downloaded, the variable ```redis.version``` from the configuration file ```hadoop-unit-default.properties``` is used.

<div id="focus-on-oozie"/>
# Focus on Oozie

To use oozie, you need:

* to download the [oozie's share libs](http://s3.amazonaws.com/public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.6.5.0/tars/oozie/oozie-4.2.0.2.6.5.0-292-distro.tar.gz)
* to edit the configuration file ```conf/hadoop-unit-default.properties``` and to set the variable ```oozie.sharelib.path``` to where you downloaded the oozie's share libs

<div id="focus-on-hive3"/>
# Focus on Hive3

Because Hive 3 is doing in the class `HiveMaterializedViewsRegistry` in the method `init()` a call to `HiveConf conf = new HiveConf();`, the properties are lost.

This is why a `hive-site.xml` has to be found in the classpath where the property `hive.metastore.uris` has to be set with the hivemetastore's thrift url.

Without this property, Hiveserver2 try to start an embedded hivemetastore which create a conflit with the hivemetastore's derby.  

<div id="focus-on-hadoop3"/>
# Focus on Hadoop 3

The maven's __Simple dependency Usage__ does not work with Hadoop 3 (hdfs, hive, yarn).

The maven's __integration plugin in mode embedded__ or the standalone mode are recommended.

<div id="focus-on-pulsar"/>
# Focus on Pulsar

The maven's __Simple dependency Usage__ does not work with Pulsar.

The maven's __integration plugin in mode embedded__ or the standalone mode are recommended.