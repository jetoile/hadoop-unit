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

public class ElasticSearchConfig {

    // ElasticSearch
    public static final String ELASTICSEARCH_IP_KEY = "elasticsearch.ip";
    public static final String ELASTICSEARCH_HTTP_PORT_KEY = "elasticsearch.http.port";
    public static final String ELASTICSEARCH_TCP_PORT_KEY = "elasticsearch.tcp.port";
    public static final String ELASTICSEARCH_VERSION = "elasticsearch.version";
    public static final String ELASTICSEARCH_INDEX_NAME = "elasticsearch.index.name";
    public static final String ELASTICSEARCH_CLUSTER_NAME = "elasticsearch.cluster.name";
    public static final String ELASTICSEARCH_DOWNLOAD_URL = "elasticsearch.download.url";

    private ElasticSearchConfig() {}
}
