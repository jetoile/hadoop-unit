package fr.jetoile.hadoopunit;

import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static fr.jetoile.hadoopunit.HadoopUnitConfig.DEFAULT_PROPS_FILE;

public class HadoopStandaloneBootstrapTest {


    @Test
    public void test() throws BootstrapException, FileNotFoundException, ConfigurationException {
        PropertiesConfiguration configuration = new PropertiesConfiguration("hadoop.properties");
        PropertiesConfiguration hadoopUnitConfiguration = new PropertiesConfiguration(DEFAULT_PROPS_FILE);


        try {
            BufferedReader br = new BufferedReader(new FileReader(configuration.getFile()));
//            String line = null;
//            while ((line = br.readLine()) != null) {
//
//            }

            Properties p = new Properties();
            p.load(br);


            Enumeration<String> enumeration = (Enumeration<String>) p.propertyNames();
            Enumeration<String> enumeration2 = (Enumeration<String>) p.propertyNames();

            System.out.println("========");

            while (enumeration2.hasMoreElements()) {
                System.out.println(enumeration2.nextElement());
            }

            System.out.println("========");

//            Collections.list(componentKeys).stream().map(String::toString).filter(c -> configuration.getBoolean(c)).collect(Collectors.toList());


            Collections.list(enumeration).stream().map(String::toString).filter(c -> configuration.getBoolean(c)).forEach(System.out::println);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}