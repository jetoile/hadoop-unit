#Usage
##Integration testing (will start each component present into classpath)
With maven, add dependencies of components which are needed

Sample:
```xml
<dependency>
    <groupId>fr.jetoile.hadoop</groupId>
    <artifactId>hadoop-unit-hdfs</artifactId>
    <version>1.1-SNAPSHOT</version>
    <scope>test</scope>
</dependency>
```

In test do:
```java
@BeforeClass
public static void setup() {
    HadoopBootstrap.INSTANCE.startAll();
}

@AfterClass
public static void tearDown() {
    HadoopBootstrap.INSTANCE.stopAll();
}
```

##WIntegration testing v2 (with specific component)
With maven, add dependencies of components which are needed

Sample:
```xml
<dependency>
    <groupId>fr.jetoile.hadoop</groupId>
    <artifactId>hadoop-unit-hdfs</artifactId>
    <version>1.1-SNAPSHOT</version>
    <scope>test</scope>
</dependency>
```

In test do:
```java
@BeforeClass
public static void setup() throws NotFoundServiceException {
    HadoopBootstrap.INSTANCE
        .start(Component.ZOOKEEPER)
        .start(Component.HDFS)
        .start(Component.HIVEMETA)
        .start(Component.HIVESERVER2)
        .startAll();
}

@AfterClass
public static void tearDown() throws NotFoundServiceException {
    HadoopBootstrap.INSTANCE
        .stop(Component.HIVESERVER2)
        .stop(Component.HIVEMETA)
        .stop(Component.HDFS)
        .stop(Component.ZOOKEEPER)
        .stopAll();
}
```

##Standalone mode
Unzip `hadoop-unit-standalone-<version>.tar.gz`
Change `conf/default.properties`
Change `conf/hadoop.properties`

Start in fg with:
```bash
./bin/hadoop-unit-standalone console
```

Start in bg with:
```bash
./bin/hadoop-unit-standalone start
```

Stop with:
```bash
./bin/hadoop-unit-standalone stop
```

##Shell Usage
Hadoop-unit can be used with common tools such as:

* hbase shell
* kafka-console command
* hdfs command
* hive shell

###Kafka-console command

* Download and unzip kafka
* From directory `KAFKA_HOME/bin` (or `KAFKA_HOME/bin/windows` for windows), execute command: 
```bash
kafka-console-consumer --zookeeper localhost:22010 --topic topic
```

###HBase Shell

* Download and unzip HBase
* set variable `HBASE_HOME`
* edit file `HBASE_HOME/conf/hbase-site.xml`:
```bash
<configuration>
	<property>
		<name>hbase.zookeeper.quorum</name>
		<value>127.0.0.1:22010</value>
	</property>
	<property>
		<name>zookeeper.znode.parent</name>
		<value>/hbase-unsecure</value>
	</property>
</configuration>
```

* From directory `HBASE_HOME/bin`, execute command: 
```bash
hbase shell
```

###HDFS command

* From directory `HADOOP_HOME/bin`, execute command: 
```bash
hdfs dfs -ls hdfs://localhost:20112/
```
###Hive Shell

* Download and unzip Hive
* edit file `HIVE_HOME/conf/hive-site.xml`:
```bash
<configuration>
	<property>
		<name>hive.metastore.uris</name>
		<value>127.0.0.1:20102</value>
	</property>
</configuration>
```
* From directory `HIVE_HOME/bin`, execute command: 
```bash
hive
```

#Sample
See hadoop-unit-standalone/src/test/java/fr/jetoile/hadoopunit/integrationtest

#Component available

* SolrCloud 5.4.1
* Kafka 0.9.0
* Hive (metastore and server2)
* Hdfs
* Zookeeper
* Oozie (WIP)
* HBase

Built on:

* hadoop-mini-cluster-0.1.3 (0.1.4 not released on central maven repo) (https://github.com/sakserv/hadoop-mini-clusters)

Use: 
* download and unzip hadoop
* download and unzip oozie (http://s3.amazonaws.com/public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.3.4.0/tars/oozie-4.2.0.2.3.4.0-3485-distro.tar.gz)
* edit default.properties and indicate HADOOP_HOME
* edit default.properties and indicate oozie.sharelib.path

Todo:
* male client utils for kafka produce/consume
* make sample with spark streaming and kafka

Issues:
* oozie does not work on windows 7 (see http://stackoverflow.com/questions/25790319/getting-access-denied-error-while-running-hadoop-2-3-mapreduce-jobs-in-windows-7)
* integrate phoenix
* can only manage one solr collection
* better docs ;)
