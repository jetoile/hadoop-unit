/**
 * User: khanh
 * To change this template use File | Settings | File Templates.
 */
module hadoop.unit.hdfs {
    requires hadoop.unit.commons.hadoop;
    requires hadoop.unit.commons;
    requires slf4j.api;
    requires hadoop.mini.clusters.hdfs;
    requires hadoop.mini.clusters.common;
    requires commons.configuration;
    requires hadoop.common;
    requires hadoop.hdfs;
    requires commons.lang;
}