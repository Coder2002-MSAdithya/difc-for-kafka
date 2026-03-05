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
import org.apache.kafka.common.message.DummyRequestData;
import org.apache.kafka.common.message.DummyResponseData;
import org.apache.kafka.common.requests.DummyRequest;
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
    private final int warmupRequestTarget;
    private final CountDownLatch warmupLatch;
    private final AtomicInteger successfulRequests = new AtomicInteger(0);

    private volatile boolean running = true;

    public DifcRequestSender(LogContext logContext,
                             KafkaClient client,
                             ProducerMetadata metadata,
                             Time time,
                             int requestTimeoutMs,
                             long retryBackoffMs,
                             int warmupRequestTarget) {
        this.log = logContext.logger(DifcRequestSender.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.warmupRequestTarget = Math.max(0, warmupRequestTarget);
        this.warmupLatch = new CountDownLatch(this.warmupRequestTarget);
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

                    DummyRequest.Builder builder = new DummyRequest.Builder(new DummyRequestData());
                    ClientRequest request = client.newClientRequest(
                            node.idString(),
                            builder,
                            now,
                            true,
                            requestTimeoutMs,
                            response -> {
                                DummyResponseData data = (DummyResponseData) response.responseBody().data();
                                System.out.println("DUMMY response: " + data.message());
                                int count = successfulRequests.incrementAndGet();
                                if (warmupLatch.getCount() > 0) {
                                    warmupLatch.countDown();
                                }
                                log.debug("DIFC successful requests so far: {}", count);
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

    public void awaitWarmup(long timeoutMs) throws InterruptedException, TimeoutException {
        if (warmupRequestTarget == 0)
            return;

        if (!warmupLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Timed out waiting for " + warmupRequestTarget
                    + " DIFC requests to be sent. Completed=" + successfulRequests.get());
        }
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