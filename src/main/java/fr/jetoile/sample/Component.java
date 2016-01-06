package fr.jetoile.sample;

import fr.jetoile.sample.component.*;

/**
 * List of component which can be bootstrap.
 * Warning : this list should be sorted
 */
public enum Component {
    ZOOKEEPER("zookeeper"),
    HDFS("hdfs"),
    HIVEMETA("hivemeta"),
    HIVESERVER2("hiveserver2"),
    KAFKA("kafka"),
    HBASE("hbase"),
    SOLRCLOUD("solrcloud"),
    SOLR("solr");

    private String key;

    Component(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

}

