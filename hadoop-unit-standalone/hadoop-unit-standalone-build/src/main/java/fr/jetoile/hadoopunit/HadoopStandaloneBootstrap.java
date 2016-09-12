/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.jetoile.hadoopunit;

import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HadoopStandaloneBootstrap {

    final private static Logger LOGGER = LoggerFactory.getLogger(HadoopStandaloneBootstrap.class);
    static private Configuration configuration;
    static private List<Component> componentsToStart = new ArrayList<>();
    static private List<ComponentProperties> componentsToStop = new ArrayList<>();
    static private List<ComponentProperties> componentsProperty = new ArrayList();


    public static void main(String[] args) throws BootstrapException, MalformedURLException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String homeDirectory = ".";
//        String homeDirectory = "/home/khanh/github/hadoop-bootstrap/hadoop-unit-standalone2/hadoop-unit-standalone2-build/target/hadoop-unit-standalone2-build-1.5-SNAPSHOT";
        if (!org.apache.commons.lang.StringUtils.isEmpty(System.getenv("HADOOP_UNIT_HOME"))) {
            homeDirectory = System.getenv("HADOOP_UNIT_HOME");
        }
        LOGGER.debug("is using {} for local directory", homeDirectory);

        try {
            configuration = new PropertiesConfiguration("hadoop.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        Arrays.asList(Component.values()).stream().forEach(c -> {
            if (configuration.containsKey(c.name().toLowerCase()) && configuration.getBoolean(c.name().toLowerCase())) {
                componentsToStart.add(c);
            }
        });


        String finalHomeDirectory = homeDirectory;
        componentsToStart.stream().forEach(c -> {
            ComponentProperties componentProperties = loadAndRun(finalHomeDirectory, c.getKey(), c.getMainClass());
            componentsProperty.add(componentProperties);
            componentsToStop.add(0, componentProperties);
        });


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("All services are going to be stopped");
                componentsToStop.stream().forEach(c -> {
                    try {
                        Method main = c.getMainClass().getMethod("stop");
                        main.invoke(c.getInstance());
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        LOGGER.error("unable to reflect main", e);
                    }
                });
            }
        });


        printBanner();
    }

    private static void printBanner() {
        HadoopUtils.printBanner(System.out);
        componentsProperty.stream().forEach(c -> {
            Object name = null;
            Object prop = null;

            try {
                Method main = c.getMainClass().getMethod("getName");
                name = main.invoke(c.getInstance());

                main = c.getMainClass().getMethod("getProperties");
                prop = main.invoke(c.getInstance());
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                LOGGER.error("unable to reflect main", e);
            }

            System.out.println("\t\t - " + name + " " + prop);
        });
        System.out.println();
    }

    private static ComponentProperties loadAndRun(String HOME, String c, String className) {
        List<URL> urls = new ArrayList();
        try {
            String libDirectory = c.toLowerCase();
            if (StringUtils.equalsIgnoreCase(libDirectory, "hivemeta") || StringUtils.equalsIgnoreCase(libDirectory, "hiveserver2")) {
                libDirectory = "hive";
            }
            for (File f : Paths.get(HOME, "lib-ext", libDirectory).toFile().listFiles()) {
                urls.add(f.toURL());
            }
        } catch (MalformedURLException e) {
            LOGGER.error("unable to find lib-ext", e);
        }

        ClassLoader classloader = new URLClassLoader(
                (URL[]) urls.toArray(new URL[0]),
                ClassLoader.getSystemClassLoader().getParent());

        // relative to that classloader, find the main class
        Class mainClass = null;
        try {
            mainClass = classloader.loadClass(className);
        } catch (ClassNotFoundException e) {
            LOGGER.error("unable to load class", e);
        }
        Method main = null;


        try {
            Thread.currentThread().setContextClassLoader(classloader);

            Object o = mainClass.getConstructor().newInstance();
            main = mainClass.getMethod("start");
            main.invoke(o);
            return new ComponentProperties(o, mainClass);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            LOGGER.error("unable to reflect main", e);
        }
        return null;


//        Object instance = null;
//        try {
//            main = mainClass.getMethod("main", new Class[]{args.getClass()});
//        } catch (NoSuchMethodException  e) {
//            LOGGER.error("unable to reflect main", e);
//        }
//
//        // well-behaved Java packages work relative to the
//        // context classloader.  Others don't (like commons-logging)
//        Thread.currentThread().setContextClassLoader(classloader);
//        try {
//            Object[] param = {null};
//            main.invoke(instance, param);
//        } catch (IllegalAccessException | InvocationTargetException e) {
//            LOGGER.error("unable to invoke main method", e);
//        }

//        try {
//            Object[] param = {null};
//            ComponentProperties componentProperties = new ComponentProperties();
//            componentProperties.setName((String) mainClass.getDeclaredMethod("getName").invoke(invoke, param));
//            componentProperties.setProperties((String) mainClass.getDeclaredMethod("getProperties").invoke(invoke, param));
//        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
//
//        }
//        return null;
    }

    private static class ComponentProperties {
        private Object instance;
        private Class mainClass;

        public ComponentProperties(Object instance, Class mainClass) {
            this.instance = instance;
            this.mainClass = mainClass;
        }

        public Object getInstance() {
            return instance;
        }

        public Class getMainClass() {
            return mainClass;
        }
    }
}
