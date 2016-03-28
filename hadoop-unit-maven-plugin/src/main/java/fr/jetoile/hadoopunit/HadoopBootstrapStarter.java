package fr.jetoile.hadoopunit;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.util.List;
import java.util.stream.Collectors;


@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST, threadSafe = false)
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

        try {
            HadoopBootstrap.INSTANCE.startAll();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
