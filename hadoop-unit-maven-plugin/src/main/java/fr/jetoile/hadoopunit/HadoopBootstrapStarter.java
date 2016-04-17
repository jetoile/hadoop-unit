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

import fr.jetoile.hadoopunit.HadoopBootstrap;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.util.List;
import java.util.stream.Collectors;


@Mojo(name = "embedded-start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST, threadSafe = false)
public class HadoopBootstrapStarter extends AbstractMojo {

    @Parameter(property = "values", required = true)
    protected List<String> values;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {

        HadoopBootstrap bootstrap = HadoopBootstrap.INSTANCE;


        bootstrap.componentsToStart = bootstrap.componentsToStart.stream().filter(c ->
                values.contains(c.getName().toUpperCase())
        ).collect(Collectors.toList());

        bootstrap.componentsToStop = bootstrap.componentsToStop.stream().filter(c ->
                values.contains(c.getName().toUpperCase())
        ).collect(Collectors.toList());

        getLog().info("is going to start hadoop unit");
        try {
            HadoopBootstrap.INSTANCE.startAll();
        } catch (Exception e) {
            getLog().error("unable to start embedded hadoop unit", e);
        }
        getLog().info("hadoop unit started");

    }
}
