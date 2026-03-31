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
package org.apache.kafka.streams.examples.difc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Minimal Kafka Streams client to observe DIFC DUMMY request/response behavior.
 *
 * <p>Usage:
 * <pre>
 *   KafkaStreamsDifcClient [bootstrapServers] [inputTopic] [outputTopic]
 * </pre>
 *
 * <p>Defaults:
 * <ul>
 *   <li>bootstrapServers: localhost:9092</li>
 *   <li>inputTopic: streams-difc-input</li>
 *   <li>outputTopic: streams-difc-output</li>
 * </ul>
 *
 * <p>While this process runs, the Kafka Streams runtime starts the DIFC background thread.
 * With the DIFC sender implementation, you should see repeated lines like:
 * <pre>
 *   DUMMY response: ...
 * </pre>
 */
public class KafkaStreamsDifcClient {

    public static void main(final String[] args) throws InterruptedException {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String inputTopic = args.length > 1 ? args[1] : "streams-difc-input";
        final String outputTopic = args.length > 2 ? args[2] : "streams-difc-output";

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-difc-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic).to(outputTopic);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-difc-shutdown-hook") {
            @Override
            public void run() {
                try {
                    streams.close(Duration.ofSeconds(10));
                } finally {
                    latch.countDown();
                }
            }
        });

        streams.setStateListener((newState, oldState) -> {
            System.out.printf("KafkaStreams state changed: %s -> %s%n", oldState, newState);
            if (newState == KafkaStreams.State.NOT_RUNNING || newState == KafkaStreams.State.ERROR) {
                latch.countDown();
            }
        });

        streams.setUncaughtExceptionHandler((throwable) -> {
            System.err.printf("Uncaught exception in thread %s%n", throwable.getMessage());
            streams.close(Duration.ofSeconds(10));
            latch.countDown();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        System.out.printf("Starting KafkaStreamsDifcClient with bootstrap=%s, input=%s, output=%s%n",
                bootstrapServers,
                inputTopic,
                outputTopic);
        System.out.println("Look for repeated 'DUMMY response: ...' output from the DIFC sender thread.");

        streams.start();
        System.out.println(streams.registerClient());
        latch.await();
    }
}