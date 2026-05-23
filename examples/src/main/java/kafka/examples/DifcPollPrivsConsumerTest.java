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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.message.RegisterClientResponseData;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Minimal consumer to exercise background {@code POLL_PRIVS_REQ} polling on {@link KafkaConsumer}.
 *
 * <p>Enable with {@link ConsumerConfig#DIFC_POLL_PRIVS_REQ_ENABLED_CONFIG} (set here by default).
 * The dedicated thread is named {@code kafka-consumer-difc-request-thread | &lt;clientId&gt;}.
 *
 * <p>Usage:
 * <pre>
 *   ./bin/kafka-run-class.sh kafka.examples.DifcPollPrivsConsumerTest \
 *       [bootstrapServers] [topic] [groupId] [clientId] [runSeconds] [extraPropertiesFile]
 * </pre>
 *
 * <p>Example (plain local broker):
 * <pre>
 *   ./bin/kafka-run-class.sh kafka.examples.DifcPollPrivsConsumerTest localhost:9092 debug-topic difc-poll-privs-test difc-poll-privs-consumer 120
 * </pre>
 *
 * <p>For SASL/SCRAM clusters, pass a properties file as the last argument (same keys as consumer config), e.g.:
 * <pre>
 *   security.protocol=SASL_PLAINTEXT
 *   sasl.mechanism=SCRAM-SHA-256
 *   sasl.jaas.config=...
 * </pre>
 *
 * <p>What to verify:
 * <ul>
 *   <li>Consumer log at INFO: {@code POLL_PRIVS_REQ pending request: ...} when the controller queue has work</li>
 *   <li>Consumer log at TRACE: {@code POLL_PRIVS_REQ queue empty} when the queue is empty</li>
 *   <li>Broker/controller logs: {@code POLL_PRIVS_REQ} handling on the dedicated controller listener</li>
 * </ul>
 */
public class DifcPollPrivsConsumerTest {
    private static final int DEFAULT_RUN_SECONDS = 0; // 0 = run until SIGINT

    public static void main(String[] args) throws Exception {
        final String bootstrap = arg(args, 0, KafkaProperties.BOOTSTRAP_SERVERS);
        final String topic = arg(args, 1, "debug-topic");
        final String groupId = arg(args, 2, "difc-poll-privs-test-group");
        final String clientId = arg(args, 3, "difc-poll-privs-consumer");
        final int runSeconds = parseIntArg(args, 4, DEFAULT_RUN_SECONDS);
        final String extraPropsPath = args.length > 5 ? args[5] : null;

        final Properties props = baseConsumerProperties(bootstrap, groupId, clientId);
        if (extraPropsPath != null) {
            loadPropertiesFile(props, Path.of(extraPropsPath));
        }

        final CountDownLatch running = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(running::countDown, "difc-poll-privs-consumer-shutdown"));

        System.out.println("========================================");
        System.out.println(" DIFC POLL_PRIVS_REQ consumer test");
        System.out.println("========================================");
        System.out.printf("bootstrap.servers     = %s%n", bootstrap);
        System.out.printf("topic                 = %s%n", topic);
        System.out.printf("group.id              = %s%n", groupId);
        System.out.printf("client.id             = %s%n", clientId);
        System.out.printf("%s = true%n", ConsumerConfig.DIFC_POLL_PRIVS_REQ_ENABLED_CONFIG);
        System.out.printf("%s = %s%n", ConsumerConfig.DIFC_POLL_PRIVS_REQ_INTERVAL_MS_CONFIG,
                props.get(ConsumerConfig.DIFC_POLL_PRIVS_REQ_INTERVAL_MS_CONFIG));
        if (extraPropsPath != null) {
            System.out.printf("extra properties file = %s%n", extraPropsPath);
        }
        System.out.printf("run duration          = %s%n",
                runSeconds > 0 ? runSeconds + "s" : "until Ctrl+C");
        System.out.println();
        System.out.println("Watch logs for thread: kafka-consumer-difc-request-thread | " + clientId);
        System.out.println("  INFO  -> POLL_PRIVS_REQ pending request (non-empty controller queue)");
        System.out.println("  TRACE -> POLL_PRIVS_REQ queue empty");
        System.out.println("========================================");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            try {
                RegisterClientResponseData registerResponse = consumer.registerClient();
                System.out.printf("registerClient -> %s%n", registerResponse.errorMessage());
            } catch (Exception e) {
                System.out.printf("registerClient skipped: %s%n", e.getMessage());
            }

            consumer.subscribe(Collections.singleton(topic));
            System.out.printf("Subscribed to %s; polling main loop + background POLL_PRIVS_REQ...%n", topic);

            final long deadlineMs = runSeconds > 0
                    ? System.currentTimeMillis() + runSeconds * 1000L
                    : Long.MAX_VALUE;

            while (running.getCount() > 0 && System.currentTimeMillis() < deadlineMs) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed topic=%s partition=%d offset=%d key=%s value=%s%n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }

        System.out.println("DifcPollPrivsConsumerTest finished.");
    }

    private static Properties baseConsumerProperties(String bootstrap, String groupId, String clientId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.DIFC_POLL_PRIVS_REQ_ENABLED_CONFIG, "true");
        props.put(ConsumerConfig.DIFC_POLL_PRIVS_REQ_INTERVAL_MS_CONFIG, "1000");
        return props;
    }

    private static void loadPropertiesFile(Properties target, Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Properties file not found: " + path);
        }
        try (InputStream in = Files.newInputStream(path)) {
            Properties extra = new Properties();
            extra.load(in);
            target.putAll(extra);
        }
    }

    private static String arg(String[] args, int index, String defaultValue) {
        return args.length > index && args[index] != null && !args[index].isBlank()
                ? args[index]
                : defaultValue;
    }

    private static int parseIntArg(String[] args, int index, int defaultValue) {
        if (args.length <= index || args[index] == null || args[index].isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(args[index]);
    }
}
