/*
 * Copyright (c) 2011 Khanh Tuong Maudoux <kmx.petals@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
            case OOZIE:
                OozieBootstrap.INSTANCE.start();
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
            case OOZIE:
                OozieBootstrap.INSTANCE.stop();
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
