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

import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.nio.file.Path;
import java.nio.file.Paths;


@Mojo(name = "stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST, threadSafe = false)
public class HadoopBootstrapRemoteStopper extends AbstractMojo {

    @Parameter(property = "hadoopUnitPath")
    protected String hadoopUnitPath;

    @Parameter(property = "outputFile")
    protected String outputFile;

    @Parameter(property = "exec")
    protected String exec;

    @Component
    private MavenProject project;

    @Component
    private MavenSession session;

    @Component
    private BuildPluginManager pluginManager;


    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        HadoopBootstrapRemoteUtils utils = new HadoopBootstrapRemoteUtils(project, session, pluginManager);


        hadoopUnitPath = utils.getHadoopUnitPath(hadoopUnitPath, getLog());

        getLog().info("is going to stop hadoop unit with executable " + ((exec == null) ? "./hadoop-unit-standalone" : exec));
        utils.operateRemoteHadoopUnit(hadoopUnitPath, outputFile, "stop", exec);
        Path hadoopLogFilePath = Paths.get(hadoopUnitPath, "wrapper.log");

        getLog().info("is going tail log file");
        utils.tailLogFileUntilFind(hadoopLogFilePath, "<-- Wrapper Stopped", getLog());
        getLog().info("hadoop unit stopped");



    }




}
