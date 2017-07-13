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
package fr.jetoile.hadoopunit.redis;

import lombok.Builder;
import lombok.Data;

import java.io.File;
import java.io.IOException;

@Data
@Builder
public class EmbeddedRedisInstaller {

    public static final String REDIS_DOWNLOAD_URL = "http://download.redis.io/releases/";

    private String downloadUrl = REDIS_DOWNLOAD_URL;
    private String version;
    private String tmpDir;
    private boolean forceCleanupInstallationDirectory;

    RedisInstaller redisInstaller;

    public EmbeddedRedisInstaller install() throws IOException, InterruptedException {
        installRedis();
        return this;
    }

    private void installRedis() throws IOException, InterruptedException {
        redisInstaller = new RedisInstaller(version, downloadUrl, forceCleanupInstallationDirectory, tmpDir);
        redisInstaller.install();
    }

    public File getExecutableFile() {
        return redisInstaller.getExecutableFile();
    }



}
