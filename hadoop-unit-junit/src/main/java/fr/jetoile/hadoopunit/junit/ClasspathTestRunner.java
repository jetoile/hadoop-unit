package fr.jetoile.hadoopunit.junit;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

public class ClasspathTestRunner extends BlockJUnit4ClassRunner {

    static ClassLoader customClassLoader;

    public ClasspathTestRunner(Class<?> clazz) throws InitializationError {
        super(loadFromCustomClassloader(clazz));
    }

    // Loads a class in the custom classloader
    private static Class<?> loadFromCustomClassloader(Class<?> clazz) throws InitializationError {
        try {
            // Only load once to support parallel tests
            if (customClassLoader == null) {
                customClassLoader = new CustomClassLoader();
            }
            return Class.forName(clazz.getName(), true, customClassLoader);
        } catch (ClassNotFoundException e) {
            throw new InitializationError(e);
        }
    }


    // Runs junit tests in a separate thread using the custom class loader
    @Override
    public void run(final RunNotifier notifier) {
        Runnable runnable = () -> {
            super.run(notifier);
        };
        Thread thread = new Thread(runnable);
        thread.setContextClassLoader(customClassLoader);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    // Custom class loader.
    // Loads classes that match pattern, otherwise load from context loader
    public static class CustomClassLoader extends URLClassLoader {

        ClassLoader parent = null;

        public CustomClassLoader() {

            super(getClasspathUrls(), null);

            parent = Thread.currentThread()
                    .getContextClassLoader();

        }

        @Override
        public synchronized Class<?> loadClass(String name) throws ClassNotFoundException {

            // Only use custom classloader for classes from these packages
            if (name.startsWith("org.elasticsearch") || name.startsWith("org.apache.lucene")) {
                Class<?> c = findLoadedClass(name);
                if (c == null) {
                    c = findClass(name);
                }
                return c;
            }

            // Otherwise load from the parent classloader
            return parent.loadClass(name);
        }


        private static URL[] getClasspathUrls() {

            // Start with the original classpath URLs
            ArrayList<URL> classpathUrls = new ArrayList<>();

            // TODO: Add the custom urls your unit test needs
            // classpathUrls.add()

            return classpathUrls.toArray(new URL[classpathUrls.size()]);
        }
    }
}