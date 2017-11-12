module hadoop.unit.commons {
    requires slf4j.api;
    requires jansi;
    requires commons.configuration;
    requires org.apache.commons.lang3;
    requires commons.io;
    exports fr.jetoile.hadoopunit;
    exports fr.jetoile.hadoopunit.component;
    exports fr.jetoile.hadoopunit.exception;
}