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
package fr.jetoile.hadoopunit.sample;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.gateway.shell.Hadoop;
import org.apache.hadoop.gateway.shell.hbase.HBase;
import org.apache.hadoop.gateway.shell.hdfs.Hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class KnoxJob {

    public void createHdfsDirectory(Hadoop hadoop) {
        Hdfs.mkdir(hadoop).dir("/hdfs/test").now();
    }

    public void createFileOnHdfs(Hadoop hadoop) {
        Hdfs.put(hadoop).text("TEST").to("/hdfs/test/sample.txt").now();
    }

    public String getFileOnHdfs(Hadoop hadoop) throws IOException {
        InputStream inputStream = Hdfs.get(hadoop).from("/hdfs/test/sample.txt").now().getStream();
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }

    public String getHBaseStatus(Hadoop hadoop) throws IOException {
        InputStream inputStream = HBase.session(hadoop).status().now().getStream();
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }

    public void createHBaseTable(Hadoop hadoop) {
        HBase.session(hadoop).table("test").create().family("family1").endFamilyDef().now();
    }


    public String getHBaseTableSchema(Hadoop hadoop) throws IOException {
        InputStream inputStream = HBase.session(hadoop).table("test").schema().now().getStream();
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }


    public void putHBaseData(Hadoop hadoop) {
        HBase.session(hadoop).table("test").row("row_id_1").store()
                .column("family1", "col1", "col_value1")
                .now();
    }

    public String readHBaseData(Hadoop hadoop) throws IOException {
        InputStream inputStream = HBase.session(hadoop).table("test").row("row_id_1")
                .query()
                .now().getStream();
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }

}
