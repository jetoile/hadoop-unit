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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class ParquetToSolrJob {
    private final SparkSession sqlContext;

    public ParquetToSolrJob(SparkSession sqlContext) {
        this.sqlContext = sqlContext;
    }

    public void run() {

        Dataset<Row> file = sqlContext.read().parquet("hdfs://localhost:20112/khanh/test_parquet/file.parquet");
        Dataset<Row> select = file.select("id", "value_s");

        String collectionName = "collection1";
        String zkHostString = "127.0.0.1:22010";
        CloudSolrClient client = new CloudSolrClient(zkHostString);


        Map<String, String> writeToSolrOpts = new HashMap();
        writeToSolrOpts.put("zkhost", zkHostString);
        writeToSolrOpts.put("collection", collectionName);

//        "zkhost" -> zkhost, "collection" -> "movielens_movies"

        select.write().format("solr").options(writeToSolrOpts).save();

//        itemDF.write.format("solr").options(writeToSolrOpts).save
//
//        JavaRDD<SolrInputDocument> solrInputDocument = (JavaRDD<SolrInputDocument>) select.toJavaRDD().map(r -> {
//            SolrInputDocument solrDoc = new SolrInputDocument("id", "value_s");
//            solrDoc.setField("id", r.getInt(0));
//            solrDoc.setField("value_s", r.getString(1));
//            return solrDoc;
//        });
//
//        SolrSupport.sendBatchToSolr(client, collectionName, solrInputDocument.);

    }
}
