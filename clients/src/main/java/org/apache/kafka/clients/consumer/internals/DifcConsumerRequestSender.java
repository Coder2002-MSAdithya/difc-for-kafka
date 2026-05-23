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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.PollPrivsReqRequestData;
import org.apache.kafka.common.message.PollPrivsReqResponseData;
import org.apache.kafka.common.requests.PollPrivsReqRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

/**
 * Background thread runnable that periodically sends {@code POLL_PRIVS_REQ} to the cluster.
 */
public class DifcConsumerRequestSender implements Runnable {
    private final Logger log;
    private final KafkaClient client;
    private final ConsumerMetadata metadata;
    private final Time time;
    private final int requestTimeoutMs;
    private final long retryBackoffMs;
    private final long pollingIntervalMs;

    private volatile boolean running = true;

    public DifcConsumerRequestSender(LogContext logContext,
                                     KafkaClient client,
                                     ConsumerMetadata metadata,
                                     Time time,
                                     int requestTimeoutMs,
                                     long retryBackoffMs,
                                     long pollingIntervalMs) {
        this.log = logContext.logger(DifcConsumerRequestSender.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.pollingIntervalMs = Math.max(1L, pollingIntervalMs);
    }

    @Override
    public void run() {
        log.debug("Starting KafkaConsumer DIFC request thread");
        try {
            while (running) {
                try {
                    long now = time.milliseconds();
                    Node node = findReadyNode(now);
                    if (node == null) {
                        client.poll(retryBackoffMs, now);
                        continue;
                    }

                    PollPrivsReqRequest.Builder builder = new PollPrivsReqRequest.Builder(new PollPrivsReqRequestData());
                    ClientRequest request = client.newClientRequest(
                            node.idString(),
                            builder,
                            now,
                            true,
                            requestTimeoutMs,
                            this::handlePollPrivsResponse
                    );
                    client.send(request, now);
                    client.poll(requestTimeoutMs, now);
                    Utils.sleep(pollingIntervalMs);
                } catch (Throwable e) {
                    log.warn("Error in KafkaConsumer DIFC request thread", e);
                    Utils.sleep(retryBackoffMs);
                }
            }
        } finally {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Failed to close DIFC consumer network client", e);
            }
            log.debug("KafkaConsumer DIFC request thread exited");
        }
    }

    private void handlePollPrivsResponse(ClientResponse response) {
        if (response == null) {
            return;
        }
        AuthenticationException authException = response.authenticationException();
        if (authException != null) {
            log.warn("POLL_PRIVS_REQ authentication failed: {}", authException.getMessage());
            return;
        }
        UnsupportedVersionException versionMismatch = response.versionMismatch();
        if (versionMismatch != null) {
            log.warn("POLL_PRIVS_REQ unsupported API version: {}", versionMismatch.getMessage());
            return;
        }
        if (response.wasDisconnected() || response.wasTimedOut()) {
            log.debug("POLL_PRIVS_REQ skipped: disconnected={}, timedOut={}",
                    response.wasDisconnected(), response.wasTimedOut());
            return;
        }
        if (!response.hasResponse()) {
            log.debug("POLL_PRIVS_REQ returned no response body");
            return;
        }
        PollPrivsReqResponseData data = (PollPrivsReqResponseData) response.responseBody().data();
        if (data.capability() >= 0 && data.tagName() != null && !data.tagName().isEmpty()) {
            log.info("POLL_PRIVS_REQ pending request: tag={}, capability={}, requester={}",
                    data.tagName(), data.capability(), data.requesterClientId());
        } else if (log.isTraceEnabled()) {
            log.trace("POLL_PRIVS_REQ queue empty (capability={})", data.capability());
        }
    }

    public void initiateClose() {
        running = false;
        client.wakeup();
    }

    private Node findReadyNode(long now) {
        metadata.requestUpdate(true);
        for (Node node : metadata.fetch().nodes()) {
            if (client.isReady(node, now) || client.ready(node, now)) {
                return node;
            }
        }
        return null;
    }
}
