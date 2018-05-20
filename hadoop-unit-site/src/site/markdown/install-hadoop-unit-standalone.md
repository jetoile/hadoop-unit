# Install Hadoop Unit Standalone

* Download it on [search.maven.org](http://search.maven.org/remotecontent?filepath=fr/jetoile/hadoop/hadoop-unit-standalone/2.7/hadoop-unit-standalone-2.7.tar.gz)
* Unzip it
* Edit files:
  * ```conf/hadoop.properties``` to activate the components you want
  * ```conf/hadoop-unit-default.properties``` to set your variables:
    * ```maven.local.repo```: set the location to your maven local repository (ex: `~/<user>/.m2/repository` for linux user or `C:/Users/<user>/.m2/repository`
    * ```maven.central.repo```: if you have a maven repository manager, set your repository

## For linux/macOS users, run:
```bash
./bin/hadoop-unit-standalone console
```

## For Windows users:
* [download](http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz), unzip apache hadoop on your OS and set your environment variable ```HADOOP_HOME``` to where you unzip apache hadoop
* edit the configuration file ```conf\logback.xml``` and delete the line: ```<withJansi>true</withJansi>```
* in a cmd terminal, launch:

```bash
cd bin
hadoop-unit-standalone.bat console
```

If you get errors which tell you that you can not write in the directory `C:\tmp` or `D:\tmp`, it is because you are not admin of you laptop. If you can create this directory and give access to it. If not possible, edit the configuration file `conf/hadoop-unit-default.properties` and set:

```bash
zookeeper.temp.dir=C:/<path where you can write>/tmp/embedded_zk
hdfs.temp.dir=C:/<path where you can write>/tmp/embedded_hdfs
hbase.root.dir=C:/<path where you can write>/tmp/embedded_hbase
oozie.tmp.dir=C:/<path where you can write>/tmp/oozie_tmp
oozie.test.dir=C:/<path where you can write>/tmp/embedded_oozie
oozie.home.dir=C:/<path where you can write>/tmp/oozie_home
cassandra.temp.dir=C:/<path where you can write>/tmp/embedded_cassandra
neo4j.temp.dir=C:/<path where you can write>/tmp/embedded_neo4j
knox.home.dir=C:/<path where you can write>/tmp/embedded_knox
alluxio.work.dir=C:/<path where you can write>/tmp/alluxio
```


If you get errors which tell you that:

```bash
jvm 1    |      WARN in ch.qos.logback.core.ConsoleAppender[STDOUT_COLOR] - Failed to create WindowsAnsiOutputStream. Falling back on the default stream. ch.qos.logback.core.util.DynamicClassLoadingException: Failed to instantiate type org.fusesource.jansi.WindowsAnsiOutputStream
jvm 1    |      at ch.qos.logback.core.util.DynamicClassLoadingException: Failed to instantiate type org.fusesource.jansi.WindowsAnsiOutputStream
jvm 1    |      at      at ch.qos.logback.core.util.OptionHelper.instantiateByClassNameAndParameter(OptionHelper.java:69)
jvm 1    |      at      at ch.qos.logback.core.util.OptionHelper.instantiateByClassNameAndParameter(OptionHelper.java:40)
jvm 1    |      at      at ch.qos.logback.core.ConsoleAppender.getTargetStreamForWindows(ConsoleAppender.java:88)
jvm 1    |      at      at ch.qos.logback.core.ConsoleAppender.start(ConsoleAppender.java:79)
jvm 1    |      at      at ch.qos.logback.core.joran.action.AppenderAction.end(AppenderAction.java:90)
jvm 1    |      at      at ch.qos.logback.core.joran.spi.Interpreter.callEndAction(Interpreter.java:309)
jvm 1    |      at      at ch.qos.logback.core.joran.spi.Interpreter.endElement(Interpreter.java:193)
jvm 1    |      at      at ch.qos.logback.core.joran.spi.Interpreter.endElement(Interpreter.java:179)
jvm 1    |      at      at ch.qos.logback.core.joran.spi.EventPlayer.play(EventPlayer.java:62)
jvm 1    |      at      at ch.qos.logback.core.joran.GenericConfigurator.doConfigure(GenericConfigurator.java:165)
jvm 1    |      at      at ch.qos.logback.core.joran.GenericConfigurator.doConfigure(GenericConfigurator.java:152)
jvm 1    |      at      at ch.qos.logback.core.joran.GenericConfigurator.doConfigure(GenericConfigurator.java:110)
jvm 1    |      at      at ch.qos.logback.core.joran.GenericConfigurator.doConfigure(GenericConfigurator.java:53)
jvm 1    |      at      at ch.qos.logback.classic.util.ContextInitializer.configureByResource(ContextInitializer.java:75)
jvm 1    |      at      at ch.qos.logback.classic.util.ContextInitializer.autoConfig(ContextInitializer.java:150)
jvm 1    |      at      at org.slf4j.impl.StaticLoggerBinder.init(StaticLoggerBinder.java:84)
jvm 1    |      at      at org.slf4j.impl.StaticLoggerBinder.<clinit>(StaticLoggerBinder.java:55)
jvm 1    |      at      at org.slf4j.LoggerFactory.bind(LoggerFactory.java:150)
jvm 1    |      at      at org.slf4j.LoggerFactory.performInitialization(LoggerFactory.java:124)
jvm 1    |      at      at org.slf4j.LoggerFactory.getILoggerFactory(LoggerFactory.java:412)
jvm 1    |      at      at org.slf4j.LoggerFactory.getLogger(LoggerFactory.java:357)
jvm 1    |      at      at org.slf4j.LoggerFactory.getLogger(LoggerFactory.java:383)
jvm 1    |      at      at fr.jetoile.hadoopunit.HadoopStandaloneBootstrap.<clinit>(HadoopStandaloneBootstrap.java:60)
jvm 1    |      at      at java.lang.Class.forName0(Native Method)
jvm 1    |      at      at java.lang.Class.forName(Unknown Source)
jvm 1    |      at      at org.tanukisoftware.wrapper.WrapperSimpleApp.<init>(WrapperSimpleApp.java:147)
jvm 1    |      at      at org.tanukisoftware.wrapper.WrapperSimpleApp.main(WrapperSimpleApp.java:485)
jvm 1    | Caused by: java.lang.reflect.InvocationTargetException
jvm 1    |      at      at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
jvm 1    |      at      at sun.reflect.NativeConstructorAccessorImpl.newInstance(Unknown Source)
jvm 1    |      at      at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(Unknown Source)
jvm 1    |      at      at java.lang.reflect.Constructor.newInstance(Unknown Source)
jvm 1    |      at      at ch.qos.logback.core.util.OptionHelper.instantiateByClassNameAndParameter(OptionHelper.java:64)
jvm 1    |      at      ... 26 common frames omitted
jvm 1    | Caused by: java.io.IOException: Could not get the screen info: L
jvm 1    |      at      at org.fusesource.jansi.WindowsAnsiOutputStream.<init>(WindowsAnsiOutputStream.java:101)
jvm 1    |      at      ... 31 common frames omitted
```
The issue is with your jansi. Edit your file `conf\logback.xml` and delete the line: ```<withJansi>true</withJansi>```