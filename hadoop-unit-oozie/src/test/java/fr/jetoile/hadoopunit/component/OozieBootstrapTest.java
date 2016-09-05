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


import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
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
import org.junit.Ignore;
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
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
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

        FileSystem hdfsFs = ((HdfsBootstrap) HadoopBootstrap.INSTANCE.getService(Component.HDFS)).getHdfsFileSystemHandle();
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
        conf.setProperty("jobTracker", "localhost:37001");
        conf.setProperty("nameNode", "hdfs://localhost:20112");

        //submit and check
        final String jobId = oozieClient.run(conf);
        WorkflowJob wf = oozieClient.getJobInfo(jobId);
        assertNotNull(wf);

        LOGGER.info("OOZIE: Workflow: {}", wf.toString());

        while (oozieClient.getJobInfo(jobId).getStatus() != WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            Thread.sleep(1000);
        }

        while (oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            System.out.println("========== job is running");
            Thread.sleep(1000);
        }

        System.out.println("=============== OOZIE: Final Workflow status" + oozieClient.getJobInfo(jobId).getStatus());
        wf = oozieClient.getJobInfo(jobId);
        System.out.println("=============== OOZIE: Workflow: {}" + wf.toString());

        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());


        hdfsFs.close();
        assertThat("true").isEqualTo("true");

    }

    @Test
    @Ignore
    public void oozieShouldStartWithShell() throws Exception {

        LOGGER.info("OOZIE: Test Submit Workflow Start");

        FileSystem hdfsFs = ((HdfsBootstrap) HadoopBootstrap.INSTANCE.getService(Component.HDFS)).getHdfsFileSystemHandle();
        OozieClient oozieClient = ((OozieBootstrap) HadoopBootstrap.INSTANCE.getService(Component.OOZIE)).getOozieClient();

        hdfsFs.mkdirs(new Path("/khanh/test"));
        hdfsFs.mkdirs(new Path("/khanh/work"));
        hdfsFs.copyFromLocalFile(new Path(OozieBootstrap.class.getClassLoader().getResource("workflow.xml").toURI()), new Path("/khanh/test/workflow.xml"));
        hdfsFs.copyFromLocalFile(new Path(OozieBootstrap.class.getClassLoader().getResource("test.sh").toURI()), new Path("/khanh/work/test.sh"));
//        hdfsFs.copyFromLocalFile(new Path(OozieBootstrap.class.getClassLoader().getResource("test3.sh").toURI()), new Path("/khanh/work/test3.sh"));


        //write job.properties
        Properties conf = oozieClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, "hdfs://localhost:20112/khanh/test/workflow.xml");
        conf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());
        conf.setProperty("jobTracker", "localhost:37001");
        conf.setProperty("nameNode", "hdfs://localhost:20112");

        //submit and check
        final String jobId = oozieClient.run(conf);

        while (oozieClient.getJobInfo(jobId).getStatus() != WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            Thread.sleep(1000);
        }

        while (oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            System.out.println("========== job is running");
            Thread.sleep(1000);
        }

        System.out.println("=============== OOZIE: Final Workflow status" + oozieClient.getJobInfo(jobId).getStatus());
        WorkflowJob wf = oozieClient.getJobInfo(jobId);
        System.out.println("=============== OOZIE: Workflow: {}" + wf.toString());

        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());

        hdfsFs.close();
        assertThat("true").isEqualTo("true");

    }


    @Test
    @Ignore
    public void oozieShouldStartWithHive() throws Exception {

        LOGGER.info("OOZIE: Test Submit Workflow Start");

        FileSystem hdfsFs = ((HdfsBootstrap) HadoopBootstrap.INSTANCE.getService(Component.HDFS)).getHdfsFileSystemHandle();
        OozieClient oozieClient = ((OozieBootstrap) HadoopBootstrap.INSTANCE.getService(Component.OOZIE)).getOozieClient();

        hdfsFs.mkdirs(new Path("/khanh/test2"));
        hdfsFs.mkdirs(new Path("/khanh/work2"));
        hdfsFs.mkdirs(new Path("/khanh/etc2"));
        hdfsFs.copyFromLocalFile(new Path(OozieBootstrap.class.getClassLoader().getResource("workflow2.xml").toURI()), new Path("/khanh/test2/workflow.xml"));
        hdfsFs.copyFromLocalFile(new Path(OozieBootstrap.class.getClassLoader().getResource("hive-site.xml").toURI()), new Path("/khanh/etc2/hive-site.xml"));
        hdfsFs.copyFromLocalFile(new Path(OozieBootstrap.class.getClassLoader().getResource("test.csv").toURI()), new Path("/khanh/work2/test.csv"));
        hdfsFs.copyFromLocalFile(new Path(OozieBootstrap.class.getClassLoader().getResource("test.hql").toURI()), new Path("/khanh/etc2/test.hql"));


        //write job.properties
        Properties conf = oozieClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, "hdfs://localhost:20112/khanh/test2/workflow.xml");
        conf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());
        conf.setProperty("jobTracker", "localhost:37001");
        conf.setProperty("nameNode", "hdfs://localhost:20112");
        conf.setProperty("hiveTry", "hdfs://localhost:20112/khanh/etc2/test.hql");
//        conf.setProperty("oozie.use.system.libpath", "true");
//        conf.setProperty("oozie.libpath", "hdfs://localhost:20112/tmp/share_lib/share/lib");

        //submit and check
        final String jobId = oozieClient.run(conf);

        while (oozieClient.getJobInfo(jobId).getStatus() != WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            Thread.sleep(1000);
        }

        while (oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            System.out.println("========== job is running");
            Thread.sleep(1000);
        }

        System.out.println("=============== OOZIE: Final Workflow status" + oozieClient.getJobInfo(jobId).getStatus());
        WorkflowJob wf = oozieClient.getJobInfo(jobId);
        System.out.println("=============== OOZIE: Workflow: {}" + wf.toString());

        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());

        hdfsFs.close();
        assertThat("true").isEqualTo("true");

    }
}
