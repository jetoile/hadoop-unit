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

import com.lucidworks.spark.SolrSupport;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class ParquetToSolrJob {
    private final JavaSparkContext sc;

    public ParquetToSolrJob(JavaSparkContext sc) {
        this.sc = sc;
    }

    public void run() {
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame file = sqlContext.read().parquet("hdfs://localhost:20112/khanh/test_parquet/file.parquet");
        DataFrame select = file.select("id", "value");

        JavaRDD<SolrInputDocument> solrInputDocument = select.toJavaRDD().map(r -> {
            SolrInputDocument solrDoc = new SolrInputDocument();
            solrDoc.setField("id", r.getInt(0));
            solrDoc.setField("value_s", r.getString(1));
            return solrDoc;
        });

        String collectionName = "collection1";
        String zkHostString = "127.0.0.1:22010";
        SolrSupport.indexDocs(zkHostString, collectionName, 1000, solrInputDocument);
    }
}
