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
