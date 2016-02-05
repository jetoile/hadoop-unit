/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package fr.jetoile.sample.kafka.consumer;

import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaTestConsumer {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestConsumer.class);

    long numRead = 0;
    private List<String> m_replicaBrokers = new ArrayList<String>();


    public void consumeMessages(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", a_seedBrokers.get(0) + ":" + a_port);
//            props.put("metadata.broker.list", a_seedBrokers.get(0) + ":" + a_port);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "100");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(a_topic));
        while (numRead == 0) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                numRead++;
            }
        }
    }


    public void consumeMessages2(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws UnsupportedEncodingException {
        SimpleConsumer simpleConsumer = new SimpleConsumer(a_seedBrokers.get(0),
                a_port,
                30000,
                2,
                "test");

        System.out.println("Testing single fetch");
        kafka.api.FetchRequest req = new FetchRequestBuilder()
                .clientId("test")
                .addFetch(a_topic, 0, 0L, 100)
                .build();
        while (numRead != 10) {
            FetchResponse fetchResponse = simpleConsumer.fetch(req);
            printMessages(fetchResponse.messageSet(a_topic, 0));
            numRead++;
        }
    }

    public long getNumRead() {
        return numRead;
    }

    private void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for(MessageAndOffset messageAndOffset: messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));
        }
    }

}
