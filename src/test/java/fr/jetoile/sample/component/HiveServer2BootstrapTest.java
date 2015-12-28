package fr.jetoile.sample.component;


import fr.jetoile.sample.BootstrapException;
import fr.jetoile.sample.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class HiveServer2BootstrapTest {


    @Before
    public void setup() throws BootstrapException {
    }


    @Test
    public void hiveServer2ShouldStart() throws InterruptedException {

//        System.setProperty("HADOOP_HOME", "/opt/hadoop");

        Bootstrap hiveMetastore = HiveMetastoreBootstrap.INSTANCE.start();

        Bootstrap hiveServer2 = HiveServer2Bootstrap.INSTANCE.start();

        assertThat(Utils.available("127.0.0.1", 20103)).isFalse();

        hiveServer2.stop();
        hiveMetastore.stop();  //TODO : ne veux pas s'arreter
//        assertThat(Utils.available("127.0.0.1", 20103)).isTrue();
    }
}