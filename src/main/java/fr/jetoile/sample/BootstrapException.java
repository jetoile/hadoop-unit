package fr.jetoile.sample;

import org.apache.commons.configuration.ConfigurationException;

public class BootstrapException extends Exception {
    public BootstrapException(String s, ConfigurationException e) {
        super(s, e);
    }

    public BootstrapException(String s) {
        super(s);
    }
}
