# Use Cli to operate Hadoop Unit

Hadoop-unit can be used with common tools such as:

* [zookeeper command](#zk-command)
* [hdfs command](#hdfs-command)
* [hive shell](#hive-shell)
* [hive beeline](#hive-beeline)


<div id="zk-command"/>
# Zookeeper command

* Download and unzip zookeeper
* From directory `ZK_HOME/bin`, execute command:

```bash
./zkCli.sh -server localhost:22010
```



<div id="hdfs-command"/>
# HDFS command

* From directory `HADOOP_HOME/bin`, execute command:

```bash
hdfs dfs -ls hdfs://localhost:20112/
```

**For windows user, you could have some issue like `-classpath is not known`. The cause of these errors are because your `JAVA_HOME` has space into. If your `JAVA_HOME` is linked to `C:\Program File\Java\...` then declared it as `C:\Progra~1\Java\...`**

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