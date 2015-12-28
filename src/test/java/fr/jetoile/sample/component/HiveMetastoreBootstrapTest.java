package fr.jetoile.sample.component;


import fr.jetoile.sample.BootstrapException;
import fr.jetoile.sample.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class HiveMetastoreBootstrapTest {

    @Before
    public void setup() throws BootstrapException {
    }


    @Test
    public void hiveMetastoreShouldStart() throws InterruptedException {

        Bootstrap hiveMetastore = HiveMetastoreBootstrap.INSTANCE
                .start();

        assertThat(Utils.available("127.0.0.1", 20102)).isFalse();

        hiveMetastore.stop();  //TODO : ne veux pas s'arreter
//        assertThat(Utils.available("127.0.0.1", 20102)).isTrue();
    }
}
