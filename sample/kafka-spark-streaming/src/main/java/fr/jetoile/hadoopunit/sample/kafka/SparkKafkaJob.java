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

package fr.jetoile.hadoopunit.sample.kafka;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SparkKafkaJob implements Serializable {
    private final JavaStreamingContext scc;
    private String zkString;
    private String topic;

    public SparkKafkaJob(JavaStreamingContext scc) {
        this.scc = scc;
    }

    public void setZkString(String zkString) {
        this.zkString = zkString;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void run() {
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, 1);

        JavaPairReceiverInputDStream<String, String> stream = KafkaUtils.createStream(
                scc,
                zkString,
                "groupId",
                topicMap);

        JavaDStream<String> messages = stream.map(r -> r._2());
        messages.foreach(r -> {
            r.foreach(t -> {
                System.out.println("========================");
                System.out.println(t);
            });
            return null;
        });
    }
}
