package fr.jetoile.sample.component;


import fr.jetoile.sample.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ZookeeperBootstrapTest {

    private static Bootstrap zookeeper;

    @BeforeClass
    public static void setup() {
        zookeeper = ZookeeperBootstrap.INSTANCE.start();
    }

    @AfterClass
    public static void tearDown() {
        zookeeper.stop();
    }


    @Test
    public void zookeeperShouldStart() throws InterruptedException {
        assertThat(Utils.available("127.0.0.1", 22010)).isFalse();
    }
}
