# Maven usage

There are 3 modes to integrate Hadoop Unit in your maven build:

* [simple dependency usage](#simple-dependency-usage)
* [use the maven integration plugin in mode embedded](#use-the-maven-integration-plugin-in-mode-embedded)
* [use the maven integration plugin in mode remote](#use-the-maven-integration-plugin-in-mode-remote)

Note that the mode **maven integration plugin in mode embedded** is the most recommanded.

<div id="simple-dependency-usage"/>
# Simple dependency Usage

With maven, add dependencies of components which are needed.

Sample:

```xml
<dependency>
    <groupId>fr.jetoile.hadoop</groupId>
    <artifactId>hadoop-unit-hdfs</artifactId>
    <version>2.10</version>
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

> **Note that because Hadoop Unit use, in this mode, the mecanism of maven dependency transitivity, if you just need, for example Hbase, you just need to indicate the dependency on the artifactId ```hadoop-unit-hbase```. Automaticaly, Hadoop Unit will start Zookeeper and Hdfs under the hood and it will start then in the right order.**


It is also possible to indicate which components you need.

```java
@BeforeClass
public static void setup() throws NotFoundServiceException {
    HadoopBootstrap.INSTANCE
        .add(Component.ZOOKEEPER)
        .add(Component.HDFS)
        .add(Component.HIVEMETA)
        .add(Component.HIVESERVER2)
        .startAll();
}

@AfterClass
public static void tearDown() throws NotFoundServiceException {
    HadoopBootstrap.INSTANCE
        .stopAll();
}
```

> **Note that, in this mode, you could have classpath issues (with guava for example) with yours tests. Indeed, because Hadoop Unit has a lot of dependencies (hadoop's hells), you may have stuff like ```NoSuchMethodError``` or ```NoClassDefFound```.
To fix these issues, use the maven's exclusion option.**

<div id="use-the-maven-integration-plugin-in-mode-embedded"/>
# Use the maven integration plugin in mode embedded

>**This is the recommended mode**


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
        <artifactId>hadoop-unit-client-hdfs</artifactId>
        <version>2.10</version>
        <scope>test</scope>
    </dependency>

    <dependency>
        <groupId>fr.jetoile.hadoop</groupId>
        <artifactId>hadoop-unit-client-hive</artifactId>
        <version>2.10</version>
        <scope>test</scope>
    </dependency>

    <dependency>
        <groupId>fr.jetoile.hadoop</groupId>
        <artifactId>hadoop-unit-client-spark</artifactId>
        <version>2.10</version>
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
            <version>${hadoop-unit.version}</version>
            <executions>
                <execution>
                    <id>start</id>
                    <goals>
                        <goal>embedded-start</goal>
                    </goals>
                    <phase>pre-integration-test</phase>
                </execution>
                <execution>
                    <id>embedded-stop</id>
                    <goals>
                        <goal>embedded-stop</goal>
                    </goals>
                    <phase>post-integration-test</phase>
                </execution>
            </executions>
            <configuration>
                <components>
                    <componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
                        <componentName>HDFS</componentName>
                        <artifactId>fr.jetoile.hadoop:hadoop-unit-hdfs:${hadoop-unit.version}</artifactId>
                    </componentArtifact>
                    <componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
                        <componentName>ZOOKEEPER</componentName>
                        <artifactId>fr.jetoile.hadoop:hadoop-unit-zookeeper:${hadoop-unit.version}</artifactId>
                    </componentArtifact>
                    <componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
                        <componentName>HIVEMETA</componentName>
                        <artifactId>fr.jetoile.hadoop:hadoop-unit-hive:${hadoop-unit.version}</artifactId>
                    </componentArtifact>
                    <componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
                        <componentName>HIVESERVER2</componentName>
                        <artifactId>fr.jetoile.hadoop:hadoop-unit-hive:${hadoop-unit.version}</artifactId>
                    </componentArtifact>
                    <componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
                        <componentName>SOLRCLOUD</componentName>
                        <artifactId>fr.jetoile.hadoop:hadoop-unit-solrcloud:${hadoop-unit.version}</artifactId>
                        <properties>
                            <solr.dir>file://${project.basedir}/src/test/resources/solr</solr.dir>
                        </properties>
                    </componentArtifact>
                </components>

            </configuration>

        </plugin>

    </plugins>
</build>
```

Values can be:

* HDFS
* ZOOKEEPER
* HIVEMETA
* HIVESERVER2
* SOLR
* SOLRCLOUD
* OOZIE
* KAFKA
* CONFLUENT_KAFKA
* CONFLUENT_SCHEMAREGISTRY
* CONFLUENT_KAFKA_REST
* CONFLUENT_KSQL_REST
* HBASE
* MONGODB
* CASSANDRA
* ELASTICSEARCH
* NEO4J
* KNOX
* ALLUXIO
* REDIS

It is also possible to override configurations with a list of `properties` which accept a map (ie. `<key>value</key>` and where `key` is a property from the file `hadoop-unit-default.properties`).

For solrcloud, it is mandatory to indicate where is the solr config:

```xml
    <componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
        <componentName>SOLRCLOUD</componentName>
        <artifactId>fr.jetoile.hadoop:hadoop-unit-solrcloud:${hadoop-unit.version}</artifactId>
        <properties>
            <solr.dir>file://${project.basedir}/src/test/resources/solr</solr.dir>
        </properties>
    </componentArtifact>
```

Here is a sample integration test:

```java
public class HdfsBootstrapIntegrationTest {

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("hadoop-unit-default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @Test
    public void hdfsShouldStart() throws Exception {

        FileSystem hdfsFsHandle = HdfsUtils.INSTANCE.getFileSystem();


        FSDataOutputStream writer = hdfsFsHandle.create(new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        reader.close();
        hdfsFsHandle.close();

        URL url = new URL(
                String.format( "http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getInt( HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY ) ) );
        URLConnection connection = url.openConnection();
        connection.setRequestProperty( "Accept-Charset", "UTF-8" );
        BufferedReader response = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);
    }
}
```

## For windows user

If you get errors which tell you that you can not write in the directory `C:\tmp` or `D:\tmp`, it is because you are not admin of you laptop. If you can create this directory and give access to it. If not possible, edit your `pom` file and for your components, set the property to the right value:

sample:

```xml
<componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
    <componentName>ZOOKEEPER</componentName>
    <artifactId>fr.jetoile.hadoop:hadoop-unit-zookeeper:${hadoop-unit.version}</artifactId>
    <properties>
      <zookeeper.temp.dir>C:/<path where you can write>/tmp/embedded_zk</zookeeper.temp.dir>
    </properties>
</componentArtifact>
<componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
    <componentName>HDFS</componentName>
    <artifactId>fr.jetoile.hadoop:hadoop-unit-hdfs:${hadoop-unit.version}</artifactId>
    <properties>
      <hdfs.temp.dir>C:/<path where you can write>/tmp/embedded_hdfs</hdfs.temp.dir>
    </properties>
</componentArtifact>
```

<div id="use-the-maven-integration-plugin-in-mode-remote"/>
# Use the maven integration plugin in mode remote

This plugin start/stop a remote local hadoop-unit-standalone.

To use it, add into the pom project stuff like that:

```xml
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
    <version>2.10</version>
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
        <hadoopUnitPath>/home/khanh/tools/hadoop-unit-standalone</hadoopUnitPath>
        <exec>./hadoop-unit-standalone</exec>
        <values>
            <value>ZOOKEEPER</value>
            <value>HDFS</value>
            <value>HIVEMETA</value>
            <value>HIVESERVER2</value>
        </values>
        <outputFile>/tmp/toto.txt</outputFile>
    </configuration>

</plugin>

<plugin>
    <artifactId>hadoop-unit-maven-plugin</artifactId>
    <groupId>fr.jetoile.hadoop</groupId>
    <version>2.10</version>
    <executions>
        <execution>
            <id>stop</id>
            <goals>
                <goal>stop</goal>
            </goals>
            <phase>post-integration-test</phase>
        </execution>
    </executions>
    <configuration>
        <hadoopUnitPath>/home/khanh/tools/hadoop-unit-standalone</hadoopUnitPath>
        <exec>./hadoop-unit-standalone</exec>
        <outputFile>/tmp/toto.txt</outputFile>
    </configuration>

</plugin>
```

Values can be:

* HDFS
* ZOOKEEPER
* HIVEMETA
* HIVESERVER2
* SOLR
* SOLRCLOUD
* OOZIE
* KAFKA
* CONFLUENT_KAFKA
* CONFLUENT_SCHEMAREGISTRY
* CONFLUENT_KAFKA_REST
* CONFLUENT_KSQL_REST
* HBASE
* MONGODB
* CASSANDRA
* ELASTICSEARCH
* NEO4J
* KNOX
* ALLUXIO
* REDIS

`hadoopUnitPath` is not mandatory but system enviroment variable `HADOOP_UNIT_HOME` must be defined.

`exec` variable is optional.

If both are set, `HADOOP_UNIT_HOME` override `hadoopUnitPath`.

*Warning: This plugin will modify hadoop.properties and delete hadoop unit logs.*

Here is a sample integration test:

```java
public class HdfsBootstrapIntegrationTest {

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("hadoop-unit-default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @Test
    public void hdfsShouldStart() throws Exception {

        FileSystem hdfsFsHandle = HdfsUtils.INSTANCE.getFileSystem();


        FSDataOutputStream writer = hdfsFsHandle.create(new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        reader.close();
        hdfsFsHandle.close();

        URL url = new URL(
                String.format( "http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getInt( HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY ) ) );
        URLConnection connection = url.openConnection();
        connection.setRequestProperty( "Accept-Charset", "UTF-8" );
        BufferedReader response = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);
    }
}
```