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
package org.apache.kafka.connect.connector;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.message.AddTagResponseData;
import org.apache.kafka.common.message.CreateTagResponseData;
import org.apache.kafka.common.message.DestroyTagResponseData;
import org.apache.kafka.common.message.GrantCapResponseData;
import org.apache.kafka.common.message.RegisterClientResponseData;
import org.apache.kafka.common.message.RemoveTagResponseData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Connectors manage integration of Kafka Connect with another system, either as an input that ingests
 * data into Kafka or an output that passes data to an external system. Implementations should
 * not use this class directly; they should inherit from {@link org.apache.kafka.connect.source.SourceConnector SourceConnector}
 * or {@link org.apache.kafka.connect.sink.SinkConnector SinkConnector}.
 * </p>
 * <p>
 * Connectors have two primary roles. First, given some configuration, they are responsible for
 * creating configurations for a set of {@link Task}s that split up the data processing. For
 * example, a database Connector might create Tasks by dividing the set of tables evenly among
 * tasks. Second, they are responsible for monitoring inputs for changes that require
 * reconfiguration and notifying the Kafka Connect runtime via the {@link ConnectorContext}. Continuing the
 * previous example, the connector might periodically check for new tables and notify Kafka Connect of
 * additions and deletions. Kafka Connect will then request new configurations and update the running
 * Tasks.
 * </p>
 */
public abstract class Connector implements Versioned {

    protected ConnectorContext context;

    public static final String DIFC_POLL_PRIVS_REQ_ENABLED_CONFIG = "difc.poll.privs.req.enabled";
    public static final String DIFC_POLL_PRIVS_REQ_INTERVAL_MS_CONFIG = "difc.poll.privs.req.interval.ms";

    private static final long DEFAULT_DIFC_POLL_PRIVS_REQ_INTERVAL_MS = 1_000L;
    private volatile ScheduledExecutorService difcPollPrivsExecutor;


    /**
     * Initialize this connector, using the provided ConnectorContext to notify the runtime of
     * input configuration changes.
     * @param ctx context object used to interact with the Kafka Connect runtime
     */
    public void initialize(ConnectorContext ctx) {
        context = ctx;
    }

    private KafkaProducer<byte[], byte[]> newDifcProducer(final Map<String, String> connectorConfigs)
    {
        final Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.putAll(connectorConfigs);
        return new KafkaProducer<>(producerConfigs);
    }

    /**
     * <p>
     * Initialize this connector, using the provided ConnectorContext to notify the runtime of
     * input configuration changes and using the provided set of Task configurations.
     * This version is only used to recover from failures.
     * </p>
     * <p>
     * The default implementation ignores the provided Task configurations. During recovery, Kafka Connect will request
     * an updated set of configurations and update the running Tasks appropriately. However, Connectors should
     * implement special handling of this case if it will avoid unnecessary changes to running Tasks.
     * </p>
     *
     * @param ctx context object used to interact with the Kafka Connect runtime
     * @param taskConfigs existing task configurations, which may be used when generating new task configs to avoid
     *                    churn in partition to task assignments
     */
    public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
        context = ctx;
        // Ignore taskConfigs. May result in more churn of tasks during recovery if updated configs
        // are very different, but reduces the difficulty of implementing a Connector
    }

    /**
     * Returns the context object used to interact with the Kafka Connect runtime.
     *
     * @return the context for this Connector.
     */
    protected ConnectorContext context() {
        return context;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    public abstract void start(Map<String, String> props);

    /**
     * Reconfigure this Connector. Most implementations will not override this, using the default
     * implementation that calls {@link #stop()} followed by {@link #start(Map)}.
     * Implementations only need to override this if they want to handle this process more
     * efficiently, e.g. without shutting down network connections to the external system.
     *
     * @param props new configuration settings
     */
    public void reconfigure(Map<String, String> props) {
        stop();
        start(props);
    }

    /**
     * Returns the {@link Task} implementation for this Connector.
     */
    public abstract Class<? extends Task> taskClass();

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most {@code maxTasks} configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    public abstract List<Map<String, String>> taskConfigs(int maxTasks);

    /**
     * Stop this connector.
     */
    public abstract void stop();

    /**
     * Validate the connector configuration values against configuration definitions.
     * @param connectorConfigs the provided configuration values
     * @return a parsed and validated {@link Config} containing any relevant validation errors with the raw
     * {@code connectorConfigs} which should prevent this configuration from being used.
     */
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigDef configDef = config();
        if (null == configDef) {
            throw new ConnectException(
                String.format("%s.config() must return a ConfigDef that is not null.", this.getClass().getName())
            );
        }
        List<ConfigValue> configValues = configDef.validate(connectorConfigs);
        return new Config(configValues);
    }

    /**
     * Define the configuration for the connector.
     * @return The ConfigDef for this connector; may not be null.
     */
    public abstract ConfigDef config();

    /**
     * Register the current connector principal/client for DIFC requests.
     */
    protected RegisterClientResponseData registerClient(final Map<String, String> connectorConfigs) {
        try (KafkaProducer<byte[], byte[]> producer = newDifcProducer(connectorConfigs)) {
            return producer.registerClient();
        }
    }

    protected CreateTagResponseData createTag(final Map<String, String> connectorConfigs, final String tagName) {
        try (KafkaProducer<byte[], byte[]> producer = newDifcProducer(connectorConfigs)) {
            return producer.createTag(tagName);
        }
    }

    protected DestroyTagResponseData destroyTag(final Map<String, String> connectorConfigs, final String tagName) {
        try (KafkaProducer<byte[], byte[]> producer = newDifcProducer(connectorConfigs)) {
            return producer.destroyTag(tagName);
        }
    }

    protected AddTagResponseData addTag(final Map<String, String> connectorConfigs, final String tagName) {
        try (KafkaProducer<byte[], byte[]> producer = newDifcProducer(connectorConfigs)) {
            return producer.addTag(tagName);
        }
    }

    protected RemoveTagResponseData removeTag(final Map<String, String> connectorConfigs, final String tagName) {
        try (KafkaProducer<byte[], byte[]> producer = newDifcProducer(connectorConfigs)) {
            return producer.removeTag(tagName);
        }
    }

    protected GrantCapResponseData requestAddCapabilityForTag(final Map<String, String> connectorConfigs,
                                                              final String tagName) {
        try (KafkaProducer<byte[], byte[]> producer = newDifcProducer(connectorConfigs)) {
            return producer.requestAddCapabilityForTag(tagName);
        }
    }

    protected GrantCapResponseData requestRemoveCapabilityForTag(final Map<String, String> connectorConfigs,
                                                                 final String tagName) {
        try (KafkaProducer<byte[], byte[]> producer = newDifcProducer(connectorConfigs)) {
            return producer.requestRemoveCapabilityForTag(tagName);
        }
    }

    /**
     * Start a background thread for periodically sending DIFC DUMMY requests when enabled.
     * Connectors should call this from {@link #start(Map)}.
     */
    /**
     * Start a background thread for periodically sending DIFC POLL_PRIVS_REQ requests when enabled.
     * Connectors should call this from {@link #start(Map)}.
     */
    public synchronized void startDifcPollPrivsPolling(final Map<String, String> connectorConfigs) {
        stopDifcPollPrivsPolling();
        if (!Boolean.parseBoolean(connectorConfigs.getOrDefault(DIFC_POLL_PRIVS_REQ_ENABLED_CONFIG, "false"))) {
            return;
        }

        final long intervalMs = parseLong(
                connectorConfigs.get(DIFC_POLL_PRIVS_REQ_INTERVAL_MS_CONFIG),
                DEFAULT_DIFC_POLL_PRIVS_REQ_INTERVAL_MS
        );

        difcPollPrivsExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, "kafka-connect-difc-poll-privs-req-thread");
            thread.setDaemon(true);
            return thread;
        });
        difcPollPrivsExecutor.scheduleAtFixedRate(() -> {
            try (KafkaProducer<byte[], byte[]> producer = newDifcProducer(connectorConfigs)) {
                producer.pollPrivsReq();
            } catch (final Throwable ignored) {
                // Keep polling thread alive; connectors may choose to implement additional logging.
            }
        }, 0L, Math.max(1L, intervalMs), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the DIFC POLL_PRIVS_REQ polling thread if one is running.
     * Connectors should call this from {@link #stop()}.
     */
    public synchronized void stopDifcPollPrivsPolling() {
        if (difcPollPrivsExecutor != null) {
            difcPollPrivsExecutor.shutdownNow();
            difcPollPrivsExecutor = null;
        }
    }

    private static long parseLong(final String value, final long fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException ignored) {
            return fallback;
        }
    }
}
