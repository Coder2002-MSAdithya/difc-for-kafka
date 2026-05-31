package org.apache.kafka.streams.processor.internals;

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
import org.apache.kafka.streams.difc.DifcPrivilegeRequestHandler;
import org.slf4j.Logger;

public class DifcStreamRequestSender implements Runnable {
    private final Logger log;
    private final KafkaClient client;
    private final Time time;
    private final int requestTimeoutMs;
    private final long retryBackoffMs;

    private volatile boolean running = true;
    private final DifcPrivilegeRequestHandler privilegeRequestHandler;

    public DifcStreamRequestSender(final LogContext logContext,
                                   final KafkaClient client,
                                   final Time time,
                                   final int requestTimeoutMs,
                                   final long retryBackoffMs) {
        this(logContext, client, time, requestTimeoutMs, retryBackoffMs, null);
    }

    public DifcStreamRequestSender(final LogContext logContext,
                                   final KafkaClient client,
                                   final Time time,
                                   final int requestTimeoutMs,
                                   final long retryBackoffMs,
                                   final DifcPrivilegeRequestHandler privilegeRequestHandler) {
        this.log = logContext.logger(DifcStreamRequestSender.class);
        this.client = client;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.privilegeRequestHandler = privilegeRequestHandler;
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

                    final PollPrivsReqRequest.Builder builder = new PollPrivsReqRequest.Builder(new PollPrivsReqRequestData());
                    final ClientRequest request = client.newClientRequest(
                            node.idString(),
                            builder,
                            now,
                            true,
                            requestTimeoutMs,
                            this::handlePollPrivsResponse
                    );

                    client.send(request, now);
                    client.poll(requestTimeoutMs, now);
                    Utils.sleep(1000);
                } catch (final Throwable e) {
                    log.warn("Error in Kafka Streams DIFC request thread", e);
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

    private void handlePollPrivsResponse(final ClientResponse response) {
        if (response == null) {
            return;
        }
        final AuthenticationException authException = response.authenticationException();
        if (authException != null) {
            log.warn("POLL_PRIVS_REQ authentication failed: {}", authException.getMessage());
            return;
        }
        final UnsupportedVersionException versionMismatch = response.versionMismatch();
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
        final PollPrivsReqResponseData data = (PollPrivsReqResponseData) response.responseBody().data();
        if (data.capability() >= 0 && data.tagName() != null && !data.tagName().isEmpty()) {
            log.info("POLL_PRIVS_REQ pending request: tag={}, capability={}, requester={}",
                    data.tagName(), data.capability(), data.requesterClientId());
            if (privilegeRequestHandler != null) {
                try {
                    privilegeRequestHandler.onPrivilegeRequest(data);
                } catch (final Exception e) {
                    log.warn("DIFC privilege request handler failed for tag={}, requester={}",
                            data.tagName(), data.requesterClientId(), e);
                }
            }
        } else if (log.isTraceEnabled()) {
            log.trace("POLL_PRIVS_REQ queue empty (capability={})", data.capability());
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
