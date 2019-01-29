#How to develop his own plugin

__This feature is only possible with Hadoop Unit v3.x.__

There are 2 classes which have to be implemented:

* a class that implements `Bootstrap` (from module `hadoop-unit-commons`) or `BootstrapHadoop` (from module `hadoop-unit-commons-hadoop`)
* a class that implements `ComponentMetadata` (from module `hadoop-unit-commons`)

Moreover, as _Service Provider Interface_ is used, the file `META-INF/services/fr.jetoile.hadoopunit.component.Bootstrap` has to be present with the reference to the class which implements the interface `Bootstrap` or `BootstrapHadoop`.

#Interface Bootstrap 

This interface define 6 methods:

* `start()` which is called when the plugin is started
* `stop()` which is called when the plugin is stopped
* `getProperties()` which is called when the banner is printed
* `loadConfig(Map<String, String> configs)` which is called to get overrided properties by the maven's plugin 
* `getMetadata()` which return the plugin's metadata as dependencies and so on
* `getName()` which is a default method which lookup into the implementation of ComponentMetadata to get the plugin name

![elastic_class](https://blog.jetoile.fr/hadoop-unit/images/elastic_class.png)

#Interface BootstrapHadoop

This interface extends the interface `Bootstrap` but declare the method `getConfiguration()` which return an instance of `org.apache.hadoop.conf.Configuration`.  

![bootstrap_class](https://blog.jetoile.fr/hadoop-unit/images/bootstrap_class.png)

#Interface ComponentMetadata

This interface define 2 methods:

* `getName()` which give the plugin's name
* `getDependencies()` which list the plugins the current plugin is dependent


![componentMetadata_class](https://blog.jetoile.fr/hadoop-unit/images/componentMetadata_class.png)

#Add a plugin to Hadoop Unit Standalone

To add a plugin to Hadoop Unit in the standalone mode, add the following information into the file `hadoop-unit-default.properties`:

* `<pluginName>.artifactId` which must have the format `<groupId>:<artifactId>:<version>`
* `<pluginName>.mainClass` which must indicate the implementation of `Bootstrap` or `BootstrapHadoop`
* `<pluginName>.metadataClass` which must indicate the implementation of `ComponentMetadata`

Note : the plugin's jar has to be in your repository manager (or on maven central) as it will be searched and downloaded.



Sample of `hadoop-unit-default.properties`:
```bash
hdfs.artifactId=fr.jetoile.hadoop:hadoop-unit-hdfs:3.1
hdfs.mainClass=fr.jetoile.hadoopunit.component.HdfsBootstrap
hdfs.metadataClass=fr.jetoile.hadoopunit.component.HdfsMetadata
```

Add your plugin's name into the file `hadoop.properties`.

Sample of `hadoop.properties`:
```bash
<pluginName>=true
```

Note that your plugin name has to be in lowercase and that his name declared into the ComponentMetadata's class has to be in uppercase.   

#Add a plugin to Hadoop Unit's maven plugin

To add a plugin to Hadoop Unit's maven plugin, add your plugin into the `configuration` element as shown below:

```xml
<build>
    <plugins>
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
                        <artifactId>hadoop-unit-hdfs</artifactId>
                        <groupId>fr.jetoile.hadoop</groupId>
                        <version>${hadoop-unit.version}</version>
                        <mainClass>fr.jetoile.hadoopunit.component.HdfsBootstrap</mainClass>
                    </componentArtifact>
                </components>
            </configuration>
        </plugin>
    </plugins>
</build>
```

Note : the plugin's jar has to be in your repository manager (or on maven central) as it will be searched and downloaded.

#HDFS Sample

This diagram shows the hdfs' plugin class diagram:
 
![hdfs_class](https://blog.jetoile.fr/hadoop-unit/images/hdfs_class.png)


This diagram shows a class diagram with multiple plugins:

![multiple_class](https://blog.jetoile.fr/hadoop-unit/images/multiple_class.png)
