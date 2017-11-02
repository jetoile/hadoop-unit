# Why Hadoop Unit

# User point of view
Because making integration tests in the Hadoop World is difficult and because working on his own laptop is quite painful, Hadoop Unit was created.

It is true that Docker is a beautiful solution. However, sometimes, it is not possible to use it.

In the Hadoop World, it is true that it is possible to run local spark but the Hadoop ecosystem (such as hbase, cassandra, hivemetastore and so on) is often needed.

Moreover, Hadoop Unit allow users to work on their laptop which bring a better feedback that having to run jobs on a development cluster. Thus they are not bored by kerberos or security complexity.


# Technical point of view

As Hadoop's components have not been designed to run in the same JVM, a lot of problems occurs.

In fact, Hadoop Unit run each component in his own classloader to avoid classpath issues (for standalone mode and maven integration plugin with mode embedded). To do this, it is using maven resolver which is the dependency engine of maven and hadoop mini cluster which allows to make integration tests for hortonworks much easier.

In fact, hadoop mini cluster is using stuff like MiniDFS, LocalOozie which are available in the Hadoop world.

For the mode [Simple dependency usage](maven-usage.html#simple-dependency-usage), a *service provider interface* is used under the hood. This is why if, for example, you need hbase, it is not provided to add zookeeper in your test dependencies since the maven dependencies transitivity is used.

Moreover, Hadoop Unit manage the right order for components' bootstrap for you.   



