package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.DummyRequestData;
import org.apache.kafka.common.message.DummyResponseData;
import org.apache.kafka.common.requests.DummyRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsMetadata;
import org.slf4j.Logger;

public class DifcStreamRequestSender implements Runnable {
    private final Logger log;
    private final KafkaClient client;
    private final StreamsMetadataState metadataState;
    private final Time time;
    private final int requestTimeoutMs;
    private final long retryBackoffMs;

    private volatile boolean running = true;

    public DifcStreamRequestSender(final LogContext logContext,
                                   final KafkaClient client,
                                   final StreamsMetadataState metadataState,
                                   final Time time,
                                   final int requestTimeoutMs,
                                   final long retryBackoffMs) {
        this.log = logContext.logger(DifcStreamRequestSender.class);
        this.client = client;
        this.metadataState = metadataState;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
    }

    @Override
    public void run() {
        log.debug("Starting Kafka Streams DIFC request thread");
        try {
            while (running) {
                try {
                    final long now = time.milliseconds();
                    final Node node = findReadyNode(now);

                    if (node == null) {
                        client.poll(retryBackoffMs, now);
                        continue;
                    }

                    final DummyRequest.Builder builder = new DummyRequest.Builder(new DummyRequestData());
                    final ClientRequest request = client.newClientRequest(
                            node.idString(),
                            builder,
                            now,
                            true,
                            requestTimeoutMs,
                            response -> {
                                final DummyResponseData data = (DummyResponseData) response.responseBody().data();
                                log.info("DUMMY response from broker: {}", data.message());
                            }
                    );

                    client.send(request, now);
                    client.poll(requestTimeoutMs, now);
                    Utils.sleep(1000);
                } catch (final Exception e) {
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
        // Reuse streams metadata host state; pick any known alive host, then rely on KafkaClient readiness
        final Node leastLoadedNode = client.leastLoadedNode(nowMs).node();
        if (leastLoadedNode != null && client.isReady(leastLoadedNode, nowMs)) {
            return leastLoadedNode;
        }

        return metadataState.allMetadata().stream()
                .map(StreamsMetadata::hostInfo)
                .filter(h -> h.host() != null)
                .map(h -> new Node(-1, h.host(), h.port()))
                .filter(n -> client.isReady(n, nowMs))
                .findFirst()
                .orElse(null);
    }
}