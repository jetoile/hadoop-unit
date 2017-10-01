package fr.jetoile.hadoopunit.sample;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
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

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:" + 20111);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "groupId");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(topic);

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        scc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, String> stringStringJavaPairDStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        stringStringJavaPairDStream.foreachRDD(r -> {
            System.out.println("========================");
            System.out.println(r);
        });

        CassandraStreamingJavaUtil.javaFunctions(stream.map(r -> {
            ObjectMapper om = new ObjectMapper();
            return om.readValue(r.value(), Order.class);
        }))
                .writerBuilder("test", "orders", CassandraJavaUtil.mapToRow(Order.class)).saveToCassandra();


    }
}