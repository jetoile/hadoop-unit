/*
 * Copyright (c) 2011 Khanh Tuong Maudoux <kmx.petals@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package fr.jetoile.hadoopunit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class HadoopUtils {


    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);
    private static Configuration configuration;

    public static void setHadoopHome() {

        if (StringUtils.isEmpty(System.getenv("HADOOP_HOME"))) {

            try {
                configuration = new PropertiesConfiguration("default.properties");
            } catch (ConfigurationException e) {
                LOG.error("unable to load default.properties", e);
            }

            String hadoop_home = configuration.getString("HADOOP_HOME");

            LOG.info("Setting hadoop.home.dir: {}", hadoop_home);
            System.setProperty("HADOOP_HOME", hadoop_home);

        } else {
            System.setProperty("HADOOP_HOME", System.getenv("HADOOP_HOME"));
        }
    }

    public static void printBanner(PrintStream out) {
        try {
            InputStream banner = HadoopUtils.class.getResourceAsStream("/banner.txt");

            BufferedReader br = new BufferedReader(new InputStreamReader(banner));
            String line = null;
            while ((line = br.readLine()) != null) {
                out.println(line);
            }
        }
        catch (Exception ex) {
            LOG.warn("Banner not printable", ex);
        }
    }
}
