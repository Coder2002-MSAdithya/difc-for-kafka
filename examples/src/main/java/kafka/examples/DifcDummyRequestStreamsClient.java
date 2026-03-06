/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.message.RegisterClientResponseData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.difc.StreamsDIFC;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * A minimal Kafka Streams sample that starts a stream topology and exercises DIFC wiring.
 *
 * <p>Usage:
 * <pre>
 *   DifcDummyRequestStreamsClient [bootstrapServers] [inputTopic] [outputTopic] [applicationId]
 * </pre>
 *
 * <p>The sample creates a pass-through topology and registers the client via {@link StreamsDIFC}
 * after startup. If your custom DIFC sender thread is correctly wired into Streams internals,
 * you should observe continuous DUMMY request/response traffic in broker logs/metrics while the
 * app is running.
 */
public final class DifcDummyRequestStreamsClient {

    private DifcDummyRequestStreamsClient() {
    }

    public static void main(final String[] args) throws InterruptedException {
        final String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        final String inputTopic = args.length > 1 ? args[1] : "difc-streams-input";
        final String outputTopic = args.length > 2 ? args[2] : "difc-streams-output";
        final String appId = args.length > 3 ? args[3] : "difc-dummy-streams-client";

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic).to(outputTopic);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("difc-streams-client-shutdown") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(10));
                shutdownLatch.countDown();
            }
        });

        try {
            streams.start();

            final RegisterClientResponseData registerResponse = StreamsDIFC.from(streams).registerClient();
            System.out.printf("DIFC registerClient response: errorCode=%d, errorMessage=%s%n",
                    registerResponse.errorCode(), registerResponse.errorMessage());

            shutdownLatch.await();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }
}