package fr.jetoile.sample.component;


import fr.jetoile.sample.BootstrapException;
import fr.jetoile.sample.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ZookeeperBootstrapTest {

    @Before
    public void setup() throws BootstrapException {
    }


    @Test
    public void zookeeperShouldStart() throws InterruptedException {

        Bootstrap zookeeper = ZookeeperBootstrap.INSTANCE
                .start();

        assertThat(Utils.available("127.0.0.1", 22010)).isFalse();

        zookeeper.stop();
        assertThat(Utils.available("127.0.0.1", 22010)).isTrue();
    }
}
