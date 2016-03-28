#Usage

When Hadoop Unit is started, it should display stuff like that:
```bash
                   ______  __      _________                         _____  __      __________
                   ___  / / /_____ ______  /___________________      __  / / /_________(_)_  /_ 1.1-SNAPSHOT
                   __  /_/ /_  __ `/  __  /_  __ \  __ \__  __ \     _  / / /__  __ \_  /_  __/
                   _  __  / / /_/ // /_/ / / /_/ / /_/ /_  /_/ /     / /_/ / _  / / /  / / /_
                   /_/ /_/  \__,_/ \__,_/  \____/\____/_  .___/      \____/  /_/ /_//_/  \__/
                                                       /_/
 		 - ZOOKEEPER [host:127.0.0.1, port:22010]
 		 - HDFS [port:20112]
 		 - HIVEMETA [port:20102]
 		 - HIVESERVER2 [port:20103]
 		 - KAFKA [host:127.0.0.1, port:20111]
 		 - HBASE [port:25111]
 		 - SOLRCLOUD [zh:127.0.0.1:22010, port:8983, collection:collection1]

```

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

##Integration testing v2 (with specific component)
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

#Maven Plugin usage
A maven plugin is provided for integration test only.

To use it, add into the pom project stuff like that:
```xml
    <dependencies>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.11</version>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>fr.jetoile.hadoop</groupId>
            <artifactId>hadoop-unit-hdfs</artifactId>
            <version>1.1-SNAPSHOT</version>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>fr.jetoile.hadoop</groupId>
            <artifactId>hadoop-unit-hive</artifactId>
            <version>1.1-SNAPSHOT</version>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>fr.jetoile.hadoop</groupId>
            <artifactId>hadoop-unit-client-hdfs</artifactId>
            <version>1.1-SNAPSHOT</version>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>fr.jetoile.hadoop</groupId>
            <artifactId>hadoop-unit-client-hive</artifactId>
            <version>1.1-SNAPSHOT</version>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>fr.jetoile.hadoop</groupId>
            <artifactId>hadoop-unit-client-spark</artifactId>
            <version>1.1-SNAPSHOT</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>**/*IntegrationTest.java</exclude>
                    </excludes>
                </configuration>
                <executions>
                    <execution>
                        <id>integration-test</id>
                        <goals>
                            <goal>test</goal>
                        </goals>
                        <phase>integration-test</phase>
                        <configuration>
                            <excludes>
                                <exclude>none</exclude>
                            </excludes>
                            <includes>
                                <include>**/*IntegrationTest.java</include>
                            </includes>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

            <plugin>
                <artifactId>hadoop-unit-maven-plugin</artifactId>
                <groupId>fr.jetoile.hadoop</groupId>
                <version>1.1-SNAPSHOT</version>
                <executions>
                    <execution>
                        <id>start</id>
                        <goals>
                            <goal>start</goal>
                        </goals>
                        <phase>pre-integration-test</phase>
                    </execution>
                </executions>
                <configuration>
                    <values>
                        <value>HDFS</value>
                        <value>ZOOKEEPER</value>
                        <value>HIVEMETA</value>
                        <value>HIVESERVER2</value>
                    </values>
                </configuration>

            </plugin>

        </plugins>
    </build>
```

Here is a sample integration test:
```java
public class HdfsBootstrapIntegrationTest {

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @Test
    public void hdfsShouldStart() throws Exception {

        FileSystem hdfsFsHandle = HdfsUtils.INSTANCE.getFileSystem();


        FSDataOutputStream writer = hdfsFsHandle.create(new Path(configuration.getString(Config.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(Config.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(new Path(configuration.getString(Config.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(Config.HDFS_TEST_STRING_KEY));
        reader.close();
        hdfsFsHandle.close();

        URL url = new URL(
                String.format( "http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getInt( Config.HDFS_NAMENODE_HTTP_PORT_KEY ) ) );
        URLConnection connection = url.openConnection();
        connection.setRequestProperty( "Accept-Charset", "UTF-8" );
        BufferedReader response = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);
    }
}
```

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
* [for oozie only] download and unzip oozie (http://s3.amazonaws.com/public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.3.4.0/tars/oozie-4.2.0.2.3.4.0-3485-distro.tar.gz)
* edit default.properties and indicate HADOOP_HOME or set your HADOOP_HOME environment variable
* edit default.properties and indicate oozie.sharelib.path

Todo:
* male client utils for kafka produce/consume
* make sample with spark streaming and kafka

Issues:
* oozie does not work on windows 7 (see http://stackoverflow.com/questions/25790319/getting-access-denied-error-while-running-hadoop-2-3-mapreduce-jobs-in-windows-7)
* integrate phoenix
* can only manage one solr collection
* better docs ;)
