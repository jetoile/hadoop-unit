module hadoop.unit.zookeeper {
    requires slf4j.api;
    requires commons.configuration;
    requires commons.lang;
    requires hadoop.unit.commons;
    requires hadoop.mini.clusters.common;
    requires hadoop.mini.clusters.zookeeper;
    requires java.desktop;

    exports fr.jetoile.hadoopunit.component.zookeeper;
}