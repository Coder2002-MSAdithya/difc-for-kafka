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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.PollPrivsReqRequestData;
import org.apache.kafka.common.message.PollPrivsReqResponseData;
import org.apache.kafka.common.requests.PollPrivsReqRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A lightweight runnable dedicated to periodically sending DUMMY requests.
 */
public class DifcRequestSender implements Runnable {
    private final Logger log;
    private final KafkaClient client;
    private final ProducerMetadata metadata;
    private final Time time;
    private final int requestTimeoutMs;
    private final long retryBackoffMs;
    private final long pollingIntervalMs;

    private volatile boolean running = true;

    public DifcRequestSender(LogContext logContext,
                             KafkaClient client,
                             ProducerMetadata metadata,
                             Time time,
                             int requestTimeoutMs,
                             long retryBackoffMs,
                             long pollingIntervalMs) {
        this.log = logContext.logger(DifcRequestSender.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.pollingIntervalMs = Math.max(1L, pollingIntervalMs);
    }

    @Override
    public void run() {
        log.debug("Starting DIFC request thread");
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
                            response -> {
                                PollPrivsReqResponseData data = (PollPrivsReqResponseData) response.responseBody().data();
                                System.out.println("POLL_PRIVS_REQ response: tag=" + data.tagName() + ", capability=" + data.capability());
                            }
                    );
                    client.send(request, now);
                    client.poll(requestTimeoutMs, now);
                    Utils.sleep(1000);
                } catch (Exception e) {
                    log.error("Error in DIFC request thread", e);
                    Utils.sleep(retryBackoffMs);
                }
            }
        } finally {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Failed to close DIFC network client", e);
            }
            log.debug("DIFC request thread exited");
        }
    }

    public void initiateClose() {
        running = false;
        client.wakeup();
    }

    private Node findReadyNode(long now) {
        metadata.requestUpdate(true);
        for (Node node : metadata.fetch().nodes()) {
            if (client.isReady(node, now)) {
                return node;
            }
        }
        return null;
    }
}