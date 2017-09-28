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
package fr.jetoile.hadoopunit;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.RemoteRepository;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Mojo(name = "embedded-start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST, threadSafe = false)
public class HadoopBootstrapStarter extends AbstractMojo {

    @Parameter(property = "port", defaultValue = "20000", required = false)
    protected int port;

    /**
     * The {@code <groupId>:<artifactId>[:<extension>[:<classifier>]]:<version>} of the artifact to resolve.
     */
    @Parameter(property = "components", required = true)
    protected List<ComponentArtifact> components;

    /**
     * The current repository/network configuration of Maven.
     *
     * @parameter default-value="${repositorySystemSession}"
     * @readonly
     */
    @Parameter(property = "repoSession", defaultValue = "${repositorySystemSession}")
    private RepositorySystemSession repoSession;

    /**
     * The project's remote repositories to use for the resolution.
     *
     * @parameter default-value="${project.remoteProjectRepositories}"
     * @readonly
     */
    @Parameter(property = "remoteRepos", defaultValue = "${project.remoteProjectRepositories}")
    private List<RemoteRepository> remoteRepos;

    private List<ComponentProperties> componentProperties = new ArrayList<>();

    private BlockingQueue<Object> queue = new ArrayBlockingQueue(1);

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        HadoopUnitRunnable hadoopUnitRunnable = new HadoopUnitRunnable(components, queue, getLog(), port, repoSession, remoteRepos);

        Thread thread = new Thread(hadoopUnitRunnable, "hadoop-unit-runner");
        thread.start();

        try {
            queue.take();
        } catch (InterruptedException e) {
            getLog().error("unable to synchronize startup: " + e.getMessage());
        }
    }
}


