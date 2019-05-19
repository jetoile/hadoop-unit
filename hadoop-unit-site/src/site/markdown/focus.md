# Focus

* [Focus on Elasticsearch](#focus-on-elasticsearch)
* [Focus on Redis](#focus-on-redis)
* [Focus on Oozie](#focus-on-oozie)

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