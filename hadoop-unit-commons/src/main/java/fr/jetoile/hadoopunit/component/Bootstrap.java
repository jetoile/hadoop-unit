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


import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

public interface Bootstrap {
    Bootstrap start();

    Bootstrap stop();

    String getProperties();

    default String getTmpDirPath(Configuration configuration, String componentTmpPathKey) {
        return configuration.getString(HadoopUnitConfig.TMP_DIR_PATH_KEY, "/tmp") + configuration.getString(componentTmpPathKey);
    }

    default String getTmpDirPath(Map<String, String> configs, String componentTmpPathKey) {
        return configs.getOrDefault(HadoopUnitConfig.TMP_DIR_PATH_KEY, "/tmp") + configs.get(componentTmpPathKey);
    }

    /**
     * Allow to override configurations when using maven plugin
     *
     * @param configs
     */
    void loadConfig(Map<String, String> configs);

    ComponentMetadata getMetadata();

    default String getName() {
        return getMetadata().getName();
    }

}
