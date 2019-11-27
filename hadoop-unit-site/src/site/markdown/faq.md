# FAQ

* [Add jar to component's classpath](#add-jar)


<div id="add-jar"/>
# Add jar to component's classpath

Because sometimes, a specific jar has to be added to a component (for example, an udf or a custom authentication), it is possible to add an extra configuration.

* For the standalone mode, add `<component name>.extraClasspath=<groupId>:<artifactId>:<version>[,<groupId>:<artifactId>:<version>]` into file `hadoop-unit-default.properties` 

ex: 

```text
pulsar.extraClasspath=com.clevercloud:biscuit-pulsar:1.0-SNAPSHOT,org.apache.logging.log4j:log4j-api:2.10.0 
```

* For maven plugin mode, add the property `<<component name>>.extraClasspath><groupId>:<artifactId>:<version>[,<groupId>:<artifactId>:<version>]</<component name>.extraClasspath>` into the component's declaration

ex:

```xml
<componentArtifact implementation="fr.jetoile.hadoopunit.ComponentArtifact">
    <componentName>PULSAR</componentName>
    <artifactId>hadoop-unit-pulsar</artifactId>
    <groupId>fr.jetoile.hadoop</groupId>
    <version>${hadoop-unit.version}</version>
    <mainClass>fr.jetoile.hadoopunit.component.PulsarBootstrap</mainClass>
    <properties>
        <pulsar.extraClasspath>com.clevercloud:biscuit-pulsar:1.0-SNAPSHOT</pulsar.extraClasspath>
    </properties>
</componentArtifact>

```