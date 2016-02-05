package fr.jetoile.sample.component;


import fr.jetoile.sample.Component;
import fr.jetoile.sample.HadoopBootstrap;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OozieBootstrapTest {

    final private Logger LOGGER = LoggerFactory.getLogger(OozieBootstrapTest.class);


    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {

        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        HadoopBootstrap.INSTANCE.startAll();

    }


    @AfterClass
    public static void tearDown() throws BootstrapException {
        HadoopBootstrap.INSTANCE.stopAll();

    }

    @Test
    public void oozieShouldStart() throws Exception {

        LOGGER.info("OOZIE: Test Submit Workflow Start");

        FileSystem hdfsFs = ((HdfsBootstrap)HadoopBootstrap.INSTANCE.getService(Component.HDFS)).getHdfsFileSystemHandle();
        OozieClient oozieClient = ((OozieBootstrap) HadoopBootstrap.INSTANCE.getService(Component.OOZIE)).getOozieClient();

        Path appPath = new Path(hdfsFs.getHomeDirectory(), "testApp");
        hdfsFs.mkdirs(new Path(appPath, "lib"));
        Path workflow = new Path(appPath, "workflow.xml");

        //write workflow.xml
        String wfApp = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='test-wf'>" +
                "    <start to='end'/>" +
                "    <end name='end'/>" +
                "</workflow-app>";

        Writer writer = new OutputStreamWriter(hdfsFs.create(workflow));
        writer.write(wfApp);
        writer.close();

        //write job.properties
        Properties conf = oozieClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, workflow.toString());
        conf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

        //submit and check
        final String jobId = oozieClient.submit(conf);
        WorkflowJob wf = oozieClient.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        LOGGER.info("OOZIE: Workflow: {}", wf.toString());
        hdfsFs.close();
        assertThat("true").isEqualTo("true");

    }
}
