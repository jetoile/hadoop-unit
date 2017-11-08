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

import java.io.File;
import java.io.IOException;

public class EmbeddedRedisInstaller {

    public static final String REDIS_DOWNLOAD_URL = "http://download.redis.io/releases/";

    private String downloadUrl = REDIS_DOWNLOAD_URL;
    private String version;
    private String tmpDir;
    private boolean forceCleanupInstallationDirectory;

    RedisInstaller redisInstaller = new RedisInstaller(version, downloadUrl, forceCleanupInstallationDirectory, tmpDir);

    EmbeddedRedisInstaller(String downloadUrl, String version, String tmpDir, boolean forceCleanupInstallationDirectory) {
        this.downloadUrl = downloadUrl;
        this.version = version;
        this.tmpDir = tmpDir;
        this.forceCleanupInstallationDirectory = forceCleanupInstallationDirectory;
    }

    public static EmbeddedRedisInstallerBuilder builder() {
        return new EmbeddedRedisInstallerBuilder();
    }

    public EmbeddedRedisInstaller install() throws IOException, InterruptedException {
        redisInstaller.install();
        return this;
    }

    public File getExecutableFile() {
        return redisInstaller.getExecutableFile();
    }


    public String getDownloadUrl() {
        return this.downloadUrl;
    }

    public String getVersion() {
        return this.version;
    }

    public String getTmpDir() {
        return this.tmpDir;
    }

    public boolean isForceCleanupInstallationDirectory() {
        return this.forceCleanupInstallationDirectory;
    }

    public static class EmbeddedRedisInstallerBuilder {
        private String downloadUrl;
        private String version;
        private String tmpDir;
        private boolean forceCleanupInstallationDirectory;

        EmbeddedRedisInstallerBuilder() {
        }

        public EmbeddedRedisInstaller.EmbeddedRedisInstallerBuilder downloadUrl(String downloadUrl) {
            this.downloadUrl = downloadUrl;
            return this;
        }

        public EmbeddedRedisInstaller.EmbeddedRedisInstallerBuilder version(String version) {
            this.version = version;
            return this;
        }

        public EmbeddedRedisInstaller.EmbeddedRedisInstallerBuilder tmpDir(String tmpDir) {
            this.tmpDir = tmpDir;
            return this;
        }

        public EmbeddedRedisInstaller.EmbeddedRedisInstallerBuilder forceCleanupInstallationDirectory(boolean forceCleanupInstallationDirectory) {
            this.forceCleanupInstallationDirectory = forceCleanupInstallationDirectory;
            return this;
        }

        public EmbeddedRedisInstaller build() {
            return new EmbeddedRedisInstaller(downloadUrl, version, tmpDir, forceCleanupInstallationDirectory);
        }
    }
}
