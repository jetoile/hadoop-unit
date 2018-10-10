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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;


@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST, threadSafe = false)
public class HadoopBootstrapRemoteStarter extends AbstractMojo {

    @Parameter(property = "values", required = true)
    protected List<String> values;

    @Parameter(property = "hadoopUnitPath")
    protected String hadoopUnitPath;

    @Parameter(property = "outputFile")
    protected String outputFile;

    @Parameter(property = "exec")
    protected String exec;

    @Parameter(property = "skip", required = false, defaultValue = "${skipTests}")
    private boolean skipTests;

    @Component
    private MavenProject project;

    @Component
    private MavenSession session;

    @Component
    private BuildPluginManager pluginManager;


    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skipTests) {
            getLog().info("Hadoop Unit's start goal is skipped");
        } else {
            HadoopBootstrapRemoteUtils utils = new HadoopBootstrapRemoteUtils(project, session, pluginManager);

            hadoopUnitPath = utils.getHadoopUnitPath(hadoopUnitPath, getLog());

            //change hadoop.properties
            getLog().info("is going to modifying hadoop.properties");
            editHadoopUnitConfFile();
            getLog().info("modifying hadoop.properties done");

            //clean log file
            Path hadoopLogFilePath = Paths.get(hadoopUnitPath, "wrapper.log");
            deleteLogFile(hadoopLogFilePath);

            getLog().info("is going to start hadoop unit with executable " + ((exec == null) ? "./hadoop-unit-standalone" : exec));
            utils.operateRemoteHadoopUnit(hadoopUnitPath, outputFile, "start", exec);

            //listen to log file and wait
            getLog().info("is going tail log file");
            utils.tailLogFileUntilFind(hadoopLogFilePath, "/_/ /_/  \\__,_/ \\__,_/  \\____/\\____/_  .___/      \\____/  /_/ /_//_/  \\__/", getLog());
            getLog().info("hadoop unit started");
        }

    }

    private void editHadoopUnitConfFile() {
        Path hadoopPropertiesPath = Paths.get(hadoopUnitPath, "conf", "hadoop.properties");
        Path hadoopPropertiesBackupPath = Paths.get(hadoopUnitPath, "conf", "hadoop.properties.old");
        if (hadoopPropertiesBackupPath.toFile().exists() && hadoopPropertiesBackupPath.toFile().canWrite()) {
            hadoopPropertiesBackupPath.toFile().delete();
        }
        hadoopPropertiesPath.toFile().renameTo(hadoopPropertiesBackupPath.toFile());

        PropertiesConfiguration configuration = new PropertiesConfiguration();

        values.forEach(v -> configuration.addProperty(v.toLowerCase(), "true"));
        try {
            configuration.save(new FileWriter(hadoopPropertiesPath.toFile()));
        } catch (ConfigurationException | IOException e) {
            getLog().error("unable to find or modifying hadoop.properties. Check user rights", e);

        }
    }


    private void deleteLogFile(Path hadoopLogFilePath) {
        getLog().info("is going to delete log file");
        if (hadoopLogFilePath.toFile().exists() && hadoopLogFilePath.toFile().canWrite()) {
            if (!hadoopLogFilePath.toFile().delete()) {
                getLog().warn("unable to delete log file");
            }
            getLog().info("delete log file done");
        }
    }


}
