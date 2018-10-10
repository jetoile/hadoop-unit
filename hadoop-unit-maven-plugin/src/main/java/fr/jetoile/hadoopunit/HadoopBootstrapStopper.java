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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

@Mojo(name = "embedded-stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST, threadSafe = false)
public class HadoopBootstrapStopper extends AbstractMojo {

    @Parameter(property = "port", defaultValue = "20000", required = false)
    protected int port;

    @Parameter(property = "timeout", defaultValue = "120000", required = false) //set timeout to 2 min
    protected int timeout;

    @Parameter(property = "skip", required = false, defaultValue = "${skipTests}")
    private boolean skipTests;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skipTests) {
            getLog().info("Hadoop Unit's embedded-stop goal is skipped");
        } else {

            getLog().info("is going to send a hadoop unit stop message");

            try (Socket client = new Socket("localhost", port);
                 PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()))) {
                client.setSoTimeout(timeout);

                out.println("stop");
                String responseLine;
                if ((responseLine = in.readLine()) != null) {
                    if (StringUtils.containsIgnoreCase(responseLine, "success")) {
                        getLog().info("hadoop unit is stopped");
                    }
                }

            } catch (IOException e) {
                getLog().error("unable to contact pre-integration phase: " + e.getMessage());
            }
        }
    }
}


