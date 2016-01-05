package fr.jetoile.sample.exception;

import org.apache.commons.configuration.ConfigurationException;

public class BootstrapException extends Exception {
    public BootstrapException(String s, ConfigurationException e) {
        super(s, e);
    }

    public BootstrapException(String s) {
        super(s);
    }

    public BootstrapException(Class<?> beanClass, String msg, Throwable cause) {
        super("Failed to instantiate [" + beanClass.getName() + "]: " + msg, cause);
    }
}
