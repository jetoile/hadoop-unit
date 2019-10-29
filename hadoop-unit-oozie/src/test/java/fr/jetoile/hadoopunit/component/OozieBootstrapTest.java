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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OozieBootstrapTest {

    final private Logger LOGGER = LoggerFactory.getLogger(OozieBootstrapTest.class);


    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws Exception {
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

        FileSystem hdfsFs = ((HdfsBootstrap) HadoopBootstrap.INSTANCE.getService("HDFS")).getHdfsFileSystemHandle();
        OozieClient oozieClient = ((OozieBootstrap) HadoopBootstrap.INSTANCE.getService("OOZIE")).getOozieClient();

        Path appPath = new Path(hdfsFs.getHomeDirectory(), "testApp");
        hdfsFs.mkdirs(new Path(appPath, "lib"));
        Path workflow = new Path(appPath, "workflow.xml");

        String testInputFile = "test_input.txt";
        String testInputDir = "/tmp/test_input_dir";
        String testOutputDir = "/tmp/test_output_dir";

        // Setup input directory and file
        hdfsFs.mkdirs(new Path(testInputDir));
        hdfsFs.copyFromLocalFile(
                new Path(getClass().getClassLoader().getResource(testInputFile).toURI()), new Path(testInputDir));

        //write workflow.xml
        String wfApp = "<workflow-app name=\"sugar-option-decision\" xmlns=\"uri:oozie:workflow:0.5\">\n" +
                "  <global>\n" +
                "    <job-tracker>${jobTracker}</job-tracker>\n" +
                "    <name-node>${nameNode}</name-node>\n" +
                "    <configuration>\n" +
                "      <property>\n" +
                "        <name>mapreduce.output.fileoutputformat.outputdir</name>\n" +
                "        <value>" + testOutputDir + "</value>\n" +
                "      </property>\n" +
                "      <property>\n" +
                "        <name>mapreduce.input.fileinputformat.inputdir</name>\n" +
                "        <value>" + testInputDir + "</value>\n" +
                "      </property>\n" +
                "    </configuration>\n" +
                "  </global>\n" +
                "  <start to=\"first\"/>\n" +
                "  <action name=\"first\">\n" +
                "    <map-reduce> <prepare><delete path=\"" + testOutputDir + "\"/></prepare></map-reduce>\n" +
                "    <ok to=\"decision-second-option\"/>\n" +
                "    <error to=\"kill\"/>\n" +
                "  </action>\n" +
                "  <decision name=\"decision-second-option\">\n" +
                "    <switch>\n" +
                "      <case to=\"option\">${doOption}</case>\n" +
                "      <default to=\"second\"/>\n" +
                "    </switch>\n" +
                "  </decision>\n" +
                "  <action name=\"option\">\n" +
                "    <map-reduce> <prepare><delete path=\"" + testOutputDir + "\"/></prepare></map-reduce>\n" +
                "    <ok to=\"second\"/>\n" +
                "    <error to=\"kill\"/>\n" +
                "  </action>\n" +
                "  <action name=\"second\">\n" +
                "    <map-reduce> <prepare><delete path=\"" + testOutputDir + "\"/></prepare></map-reduce>\n" +
                "    <ok to=\"end\"/>\n" +
                "    <error to=\"kill\"/>\n" +
                "  </action>\n" +
                "  <kill name=\"kill\">\n" +
                "    <message>\n" +
                "      Failed to workflow, error message[${wf: errorMessage (wf: lastErrorNode ())}]\n" +
                "    </message>\n" +
                "  </kill>\n" +
                "  <end name=\"end\"/>\n" +
                "</workflow-app>";

        Writer writer = new OutputStreamWriter(hdfsFs.create(workflow));
        writer.write(wfApp);
        writer.close();

        //write job.properties
        Properties oozieConf = oozieClient.createConfiguration();
        oozieConf.setProperty(OozieClient.APP_PATH, workflow.toString());
        oozieConf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());
        oozieConf.setProperty("jobTracker", "localhost:37001");
        oozieConf.setProperty("nameNode", "hdfs://localhost:20112");
        oozieConf.setProperty("user.name", System.getProperty("user.name"));
        oozieConf.setProperty("doOption", "true");

        //submit and check
        final String jobId = oozieClient.run(oozieConf);
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
    }
}
