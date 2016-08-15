package fr.jetoile.hadoopunit.sample;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class SparkKafkaJob implements Serializable {

    private static final long serialVersionUID = 1L;
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

        JavaPairReceiverInputDStream<String, String> stream = KafkaUtils.createStream(scc, zkString, "groupId", topicMap);

        JavaDStream<String> messages = stream.map(r -> r._2());
        messages.foreach(r -> {
            r.foreach(t -> {
                System.out.println("========================");
                System.out.println(t);
            });
            return null;
        });

        CassandraStreamingJavaUtil.javaFunctions(stream.map(r -> {
            ObjectMapper om = new ObjectMapper();
            return om.readValue(r._2(), Order.class);
        }))
                .writerBuilder("test", "orders", CassandraJavaUtil.mapToRow(Order.class)).saveToCassandra();


    }
}