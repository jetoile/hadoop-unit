Welcome to the Hadoop Unit wiki!

```bash
   ______  __      _________                         _____  __      __________
   ___  / / /_____ ______  /___________________      __  / / /_________(_)_  /_ 3.3
   __  /_/ /_  __ `/  __  /_  __ \  __ \__  __ \     _  / / /__  __ \_  /_  __/
   _  __  / / /_/ // /_/ / / /_/ / /_/ /_  /_/ /     / /_/ / _  / / /  / / /_
   /_/ /_/  \__,_/ \__,_/  \____/\____/_  .___/      \____/  /_/ /_//_/  \__/
                                       /_/
- HDFS
		 host:localhost
		 port:20112
		 httpPort:50070
- ZOOKEEPER
		 host:127.0.0.1
		 port:22010
- HIVEMETA
		 port:20102
- HIVESERVER2
		 port:20103
- KAFKA
		 host:127.0.0.1
		 port:20111
- HBASE
		 port:25111
		 restPort:28000
- SOLRCLOUD
		 zh:127.0.0.1:22010
		 port:8983
		 collection:collection1
- CASSANDRA
		 ip:127.0.0.1
		 port:13433
- ELASTICSEARCH
		 clusterName:elasticsearch
		 ip:127.0.0.1
		 httpPort:14433
		 tcpPort:14533
		 indexName:test_index
		 version:6.2.4
- CONFLUENT_KAFKA 
		 kafka host:127.0.0.1
		 kafka port:22222
- CONFLUENT_SCHEMAREGISTRY 
		 schemaregistry host:127.0.0.1
		 schemaregistry port:8081
- CONFLUENT_KAFKA_REST 
		 rest host:127.0.0.1
		 rest port:8082
- DOCKER_COMPOSE 
		 dockerComposeFile:/home/khanh/tmp/hadoop-unit-standalone-3.3/conf/docker-compose.yml
		 exposedPorts:{}
- DOCKER 
		 imageName:alpine:3.2
		 exposedPorts:[80]
		 fixedExposedPortsList:{21300=80}
		 envs:{MAGIC_NUMBER=42}
		 labels:{MAGIC_NUMBER=42}
		 command:[/bin/sh, -c, while true; do echo "$MAGIC_NUMBER" | nc -l -p 80; done]
		 classpathResourceMappings:{}
		 
		 
...
```

* [What is Hadoop Unit](what-is-hadoop-unit.html)

* [Why Hadoop Unit](why-hadoop-unit.html)

* Installation

  * [Install the standalone Hadoop Unit mode](install-hadoop-unit-standalone.html)
  * [Integrate Hadoop Unit 2.x in your maven project](maven-usage_2.x.html)
  * [Integrate Hadoop Unit 3.x in your maven project](maven-usage_3.x.html)

* [Use CLI to operate Hadoop Unit Standalone](cli.html)

* [How to write integration test with Hadoop Unit](howto-integrationtest.html)

* [Why Hadoop Unit v3](why-hadoopunit-v3.html)

* [Develop your own plugin](plugin-development.html)

* [Build](howto-build.html)

* [Focus](focus.html)

* [Licence](licence.html)

