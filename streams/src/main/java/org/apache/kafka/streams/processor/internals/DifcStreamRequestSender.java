package org.apache.kafka.streams.processor.internals;

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

public class DifcStreamRequestSender implements Runnable {
    private final Logger log;
    private final KafkaClient client;
    private final Time time;
    private final int requestTimeoutMs;
    private final long retryBackoffMs;

    private volatile boolean running = true;

    public DifcStreamRequestSender(final LogContext logContext,
                                   final KafkaClient client,
                                   final Time time,
                                   final int requestTimeoutMs,
                                   final long retryBackoffMs) {
        this.log = logContext.logger(DifcStreamRequestSender.class);
        this.client = client;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
    }

    @Override
    public void run() {
        System.out.println("Starting Kafka Streams DIFC request thread");
        System.out.println(running);
        try {
            while (running) {
                try {
                    final long now = time.milliseconds();
                    final Node node = findReadyNode(now);

                    if (node == null) {
                        client.poll(retryBackoffMs, now);
                        continue;
                    }

                    final PollPrivsReqRequest.Builder builder = new PollPrivsReqRequest.Builder(new PollPrivsReqRequestData());
                    final ClientRequest request = client.newClientRequest(
                            node.idString(),
                            builder,
                            now,
                            true,
                            requestTimeoutMs,
                            response -> {
                                final PollPrivsReqResponseData data = (PollPrivsReqResponseData) response.responseBody().data();
                                System.out.println("POLL_PRIVS_REQ response from broker for streams: tag=" + data.tagName() + ", capability=" + data.capability());
                            }
                    );

                    client.send(request, now);
                    client.poll(requestTimeoutMs, now);
                    Utils.sleep(1000);
                } catch (final Throwable e) {
                    log.error("Error in Kafka Streams DIFC request thread", e);
                    Utils.sleep(retryBackoffMs);
                }
            }
        } finally {
            try {
                client.close();
            } catch (final Exception e) {
                log.warn("Failed to close DIFC Streams KafkaClient", e);
            }
            log.debug("Kafka Streams DIFC request thread exited");
        }
    }

    public void initiateClose() {
        running = false;
        client.wakeup();
    }

    private Node findReadyNode(final long nowMs) {
        final Node node = client.leastLoadedNode(nowMs).node();
        if (node == null) {
            return null;
        }

        if (client.isReady(node, nowMs) || client.ready(node, nowMs)) {
            return node;
        }

        return null;
    }
}