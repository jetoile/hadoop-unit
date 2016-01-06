package fr.jetoile.sample.component;


import org.apache.hadoop.conf.Configuration;

public interface Bootstrap {
    Bootstrap start();

    Bootstrap stop();

    Configuration getConfiguration();


}
