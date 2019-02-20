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

public class DockerConfig {

    public static final String DOCKER_IMAGENAME_KEY = "docker.imagename";
    public static final String DOCKER_EXPOSEDPORTS_KEY = "docker.exposedports";
    public static final String DOCKER_ENVS_KEY = "docker.envs";
    public static final String DOCKER_LABELS_KEY = "docker.labels";
    public static final String DOCKER_COMMAND_KEY = "docker.command";
    public static final String DOCKER_FIXED_EXPOSEDPORTS_KEY = "docker.fixed.exposedports";
    public static final String DOCKER_CLASSPATH_RESOURCES_MAPPING_KEY = "docker.classpath.resources.mapping";


    private DockerConfig() {
    }
}
