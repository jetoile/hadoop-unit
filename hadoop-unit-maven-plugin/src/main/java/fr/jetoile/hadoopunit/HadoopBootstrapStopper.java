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

import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.RemoteRepository;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Mojo(name = "embedded-stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST, threadSafe = false)
public class HadoopBootstrapStopper extends AbstractMojo {

    @Parameter(property = "port", defaultValue = "20000", required = false)
    protected int port;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("is going to send a hadoop unit stop message");

        Socket client;
        try {
            client = new Socket("localhost", port);
            DataOutputStream outToServer = new DataOutputStream(client.getOutputStream());
            DataInputStream fromServer = new DataInputStream(client.getInputStream());

            outToServer.writeBytes("stop" + '\n');
            String responseLine;
            if ((responseLine = fromServer.readLine()) != null) {
                if (StringUtils.equalsIgnoreCase(responseLine, "success")) {
                    getLog().info("hadoop unit is stopped");
                }
            }

            client.close();
        }
        catch (IOException e) {
            getLog().error("unable to contact pre-integration phase: " + e.getMessage());
        }
    }
}


