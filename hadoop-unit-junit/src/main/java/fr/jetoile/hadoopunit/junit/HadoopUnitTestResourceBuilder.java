/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package fr.jetoile.hadoopunit.junit;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HadoopUnitTestResourceBuilder implements TestRule {
    private Map<String, String> properties = new HashMap<>();
    private List<ComponentArtifact> components = new ArrayList<>();
    private String path;
    private String repositoryManager = "https://repo.maven.apache.org/maven2/";

    public static HadoopUnitTestResourceBuilder forJunit() {
        return new HadoopUnitTestResourceBuilder();
    }

    public HadoopUnitTestResourceBuilder with(String name, String groupId, String artifactId, String version) {
        ComponentArtifact componentArtifact = new ComponentArtifact(name, groupId, artifactId, version);
        this.components.add(componentArtifact);
        return this;
    }

    public HadoopUnitTestResourceBuilder whereMavenLocalRepo(String path) {
        this.path = path;
        return this;
    }

    public HadoopUnitTestResourceBuilder whereRepositoryManager(String repositoryManager) {
        this.repositoryManager = repositoryManager;
        return this;
    }

    public HadoopUnitTestResourceBuilder withProperties(String key, String value) {
        properties.put(key, value);
        return this;
    }

    public HadoopUnitTestResource build() {
        HadoopUnitTestResource hadoopUnitTestResource = new HadoopUnitTestResource(repositoryManager, path, components);
        try {
            hadoopUnitTestResource.start();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return hadoopUnitTestResource;

    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                HadoopUnitTestResource hadoopUnitTestResource = new HadoopUnitTestResource(repositoryManager, path, components);

                hadoopUnitTestResource.start();

                base.evaluate();

                hadoopUnitTestResource.stop();
            }
        };
    }
}
