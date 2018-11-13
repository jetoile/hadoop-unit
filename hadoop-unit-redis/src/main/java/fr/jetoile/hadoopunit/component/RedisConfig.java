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

public class RedisConfig {

    //Redis
    public static final String REDIS_PORT_KEY = "redis.port";
    public static final String REDIS_DOWNLOAD_URL_KEY = "redis.download.url";
    public static final String REDIS_VERSION_KEY = "redis.version";
    public static final String REDIS_CLEANUP_INSTALLATION_KEY = "redis.cleanup.installation";
    public static final String REDIS_TYPE_KEY = "redis.type";
    public static final String REDIS_SLAVE_PORT_KEY = "redis.slave.ports";
    public static final String REDIS_SENTINEL_PORT_KEY = "redis.sentinel.ports";
    public static final String REDIS_TMP_DIR_KEY = "redis.temp.dir";


    private RedisConfig() {}
}
