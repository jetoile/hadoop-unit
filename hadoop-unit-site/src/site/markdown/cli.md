# Use Cli to operate Hadoop Unit

Hadoop-unit can be used with common tools such as:

* [hdfs command](#hdfs-command)
* [kafka-console command](#kafka-console-command)
* [hbase shell](#hbase-shell)
* [hive shell](#hive-shell)
* [hive beeline](#hive-beeline)
* [cassandra shell](#cassandra-shell)
* [alluxio shell](#alluxio-shell)

<div id="hdfs-command"/>
# HDFS command

* From directory `HADOOP_HOME/bin`, execute command:

```bash
hdfs dfs -ls hdfs://localhost:20112/
```

**For windows user, you could have some issue like `-classpath is not known`. The cause of these errors are because your `JAVA_HOME` has space into. If your `JAVA_HOME` is linked to `C:\Program File\Java\...` then declared it as `C:\Progra~1\Java\...`

<div id="kafka-console-command"/>
# Kafka-console command

* Download and unzip kafka
* From directory `KAFKA_HOME/bin` (or `KAFKA_HOME/bin/windows` for windows), execute command:

```bash
kafka-console-consumer --zookeeper localhost:22010 --topic topic
```
<div id="hbase-shell"/>
# HBase Shell

* Download and unzip HBase
* set variable `HBASE_HOME`
* edit file `HBASE_HOME/conf/hbase-site.xml`:
```xml
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
<div id="hive-shell"/>
# Hive Shell

* Download and unzip Hive
* Edit file `HIVE_HOME/conf/hive-site.xml`:

```xml
<configuration>
	<property>
		<name>hive.metastore.uris</name>
		<value>thrift://127.0.0.1:20102</value>
	</property>
</configuration>
```

* From directory `HIVE_HOME/bin`, execute command:

```bash
hive
```
<div id="hive-beeline"/>
# Hive beeline

*For linux/MacOS user only*

* Download and unzip Hive
* From directory `HIVE_HOME/bin`, execute command:

```bash
beeline
```
* When prompted, fill:

```bash
!connect jdbc:hive2://localhost:20103 user password
```

<div id="cassandra-shell"/>
# Cassandra Shell

* Download and unzip cassandra
* From directory `CASSANDRA_HOME/bin`, execute command:

```bash
./cqlsh localhost 13433
```

<div id="alluxio-shell"/>
# Alluxio Shell

* Download and unzip alluxio
* edit file `ALLUXIO_HOME/conf/alluxio-env.sh`:

```properties
ALLUXIO_MASTER_HOSTNAME=localhost
```

* edit file `ALLUXIO_HOME/conf/alluxio-site.properties`:

```properties
alluxio.master.port=14001
```

* From directory `ALLUXIO_HOME/bin`, execute command:

```bash
./alluxio fs ls <path>
```
