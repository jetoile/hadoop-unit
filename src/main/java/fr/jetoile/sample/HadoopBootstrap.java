package fr.jetoile.sample;

import com.google.common.collect.Lists;
import fr.jetoile.sample.component.*;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HadoopBootstrap {

    final private static Logger LOGGER = LoggerFactory.getLogger(HadoopBootstrap.class);


    private Configuration configuration;
    private List<Component> componentsToStart = new ArrayList<>();
    private List<Component> componentsToStop = new ArrayList<>();

    public HadoopBootstrap(Component... components) throws BootstrapException {

        Arrays.asList(components).stream().forEach(c -> {
                componentsToStart.add(c);
        });

        componentsToStop = Lists.newArrayList(this.componentsToStart);
        Collections.reverse(componentsToStop);
    }

    public HadoopBootstrap() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("hadoop.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        Arrays.asList(Component.values()).stream().forEach(c -> {
            if (configuration.containsKey(c.getKey()) && configuration.getBoolean(c.getKey())) {
                componentsToStart.add(Component.valueOf(c.name()));
            }
        });

        componentsToStop = Lists.newArrayList(this.componentsToStart);
        Collections.reverse(componentsToStop);
    }

    public void startAll() {
        componentsToStart.stream().forEach(c -> {
            startService(c);
        });
    }

    public void stopAll() {
        componentsToStop.stream().forEach(c -> {
            stopService(c);
        });
    }

    private void startService(Component c) {
        switch (c) {
            case HDFS:
                HdfsBootstrap.INSTANCE.start();
                break;
            case HIVEMETA:
                HiveMetastoreBootstrap.INSTANCE.start();
                break;
            case HIVESERVER2:
                HiveServer2Bootstrap.INSTANCE.start();
                break;
            case ZOOKEEPER:
                ZookeeperBootstrap.INSTANCE.start();
                break;
            case KAFKA:
                KafkaBootstrap.INSTANCE.start();
                break;
            case HBASE:
                HBaseBootstrap.INSTANCE.start();
                break;
            case SOLRCLOUD:
                SolrCloudBootstrap.INSTANCE.start();
                break;
            case SOLR:
                SolrBootstrap.INSTANCE.start();
                break;
        }
    }

    private void stopService(Component c) {
        switch (c) {
            case HDFS:
                HdfsBootstrap.INSTANCE.stop();
                break;
            case HIVEMETA:
                HiveMetastoreBootstrap.INSTANCE.stop();
                break;
            case HIVESERVER2:
                HiveServer2Bootstrap.INSTANCE.stop();
                break;
            case ZOOKEEPER:
                ZookeeperBootstrap.INSTANCE.stop();
                break;
            case KAFKA:
                KafkaBootstrap.INSTANCE.stop();
                break;
            case HBASE:
                HBaseBootstrap.INSTANCE.stop();
                break;
            case SOLRCLOUD:
                SolrCloudBootstrap.INSTANCE.stop();
                break;
            case SOLR:
                SolrBootstrap.INSTANCE.stop();
                break;
        }
    }

    public static void main(String[] args) throws BootstrapException {
        HadoopBootstrap bootstrap = new HadoopBootstrap();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("All services are going to be stopped");
                bootstrap.stopAll();
            }
        });


        bootstrap.startAll();


    }


}
