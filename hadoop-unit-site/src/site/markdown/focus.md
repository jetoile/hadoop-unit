# Focus

* [Focus on Hive 3](#focus-on-hive3)
* [Focus on Hadodp 3](#focus-on-hadoop3)


<div id="focus-on-hive3"/>
# Focus on Hive3

Because Hive 3 is doing in the class `HiveMaterializedViewsRegistry` in the method `init()` a call to `HiveConf conf = new HiveConf();`, the properties are lost.

This is why a `hive-site.xml` has to be found in the classpath where the property `hive.metastore.uris` has to be set with the hivemetastore's thrift url.

Without this property, Hiveserver2 try to start an embedded hivemetastore which create a conflit with the hivemetastore's derby.  

<div id="focus-on-hadoop3"/>
# Focus on Hadoop 3

The maven's __Simple dependency Usage__ does not work with Hadoop 3 (hdfs, hive, yarn).

The maven's __integration plugin in mode embedded__ or the standalone mode are recommended.
