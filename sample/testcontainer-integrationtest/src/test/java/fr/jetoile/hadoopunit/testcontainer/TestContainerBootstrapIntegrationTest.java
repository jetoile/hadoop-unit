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
package fr.jetoile.hadoopunit.testcontainer;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


public class TestContainerBootstrapIntegrationTest {

    @Test
    public void testStartAndStopRedis() {
        Jedis jedis = new Jedis("127.0.0.1", 21300);

        jedis.hset("test", "foo", "FOO");
        String foundObject = jedis.hget("test", "foo");

        assertThat(foundObject).isEqualTo("FOO");

        jedis.close();
    }

}