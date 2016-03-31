package fr.jetoile.hadoopunit;

import org.apache.commons.lang.StringUtils;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.twdata.maven.mojoexecutor.MojoExecutor.*;


@Mojo(name = "stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST, threadSafe = false)
public class HadoopBootstrapRemoteStopper extends AbstractMojo {

    @Parameter(property = "hadoopUnitPath")
    protected String hadoopUnitPath;

    @Parameter(property = "outputFile")
    protected String outputFile;

    @Component
    private MavenProject project;

    @Component
    private MavenSession session;

    @Component
    private BuildPluginManager pluginManager;


    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        HadoopBootstrapRemoteUtils utils = new HadoopBootstrapRemoteUtils(project, session, pluginManager);


        utils.getHadoopUnitPath(hadoopUnitPath, getLog());

        getLog().info("is going to stop hadoop unit");
        utils.operateRemoteHadoopUnit(hadoopUnitPath, outputFile, "stop");
        Path hadoopLogFilePath = Paths.get(hadoopUnitPath, "wrapper.log");

        getLog().info("is going tail log file");
        utils.tailLogFileUntilFind(hadoopLogFilePath, "<-- Wrapper Stopped", getLog());
        getLog().info("hadoop unit stopped");



    }




}
