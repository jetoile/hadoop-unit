/**
 * User: khanh
 * To change this template use File | Settings | File Templates.
 */
module hadoop.unit.kafka {
    requires hadoop.unit.commons;
    requires slf4j.api;
    requires hadoop.mini.clusters.kafka;
    requires hadoop.mini.clusters.common;
    requires commons.configuration;
    requires commons.lang;

    exports fr.jetoile.hadoopunit.component.kafka;
}