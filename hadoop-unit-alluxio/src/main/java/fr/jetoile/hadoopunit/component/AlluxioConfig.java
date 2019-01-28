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

public class AlluxioConfig {

    // Alluxio
    public static final String ALLUXIO_WORK_DIR = "alluxio.work.dir";
    public static final String ALLUXIO_HOSTNAME = "alluxio.hostname";
    public static final String ALLUXIO_MASTER_RPC_PORT = "alluxio.master.port";
    public static final String ALLUXIO_MASTER_WEB_PORT = "alluxio.master.web.port";
    public static final String ALLUXIO_PROXY_WEB_PORT = "alluxio.proxy.web.port";
    public static final String ALLUXIO_WORKER_RPC_PORT = "alluxio.worker.port";
    public static final String ALLUXIO_WORKER_DATA_PORT = "alluxio.worker.data.port";
    public static final String ALLUXIO_WORKER_WEB_PORT = "alluxio.worker.web.port";
    public static final String ALLUXIO_WEBAPP_DIRECTORY = "alluxio.webapp.directory";

    private AlluxioConfig() {}
}
