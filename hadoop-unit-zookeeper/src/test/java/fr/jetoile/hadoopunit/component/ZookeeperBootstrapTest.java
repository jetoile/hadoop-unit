package fr.jetoile.hadoopunit.component;


import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.Utils;
import org.fest.assertions.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ZookeeperBootstrapTest {

    @BeforeClass
    public static void setup() {
        HadoopBootstrap.INSTANCE.startAll();
    }

    @AfterClass
    public static void tearDown() {
        HadoopBootstrap.INSTANCE.stopAll();
    }


    @Test
    public void zookeeperShouldStart() throws InterruptedException {
        Assertions.assertThat(Utils.available("127.0.0.1", 22010)).isFalse();
    }
}
