package org.apache.kafka.security.agent.policy;

import java.util.List;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.security.agent.AppClientPolicyTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class AppClientPolicyTrackerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach
  void reset() {
    AppClientPolicyTracker.clearStateForTests();
  }

  @Test
  void producerSendMergedIntoPolicyGraph() throws Exception {
    AppClientPolicyTracker.recordProducerSendBinding(
        "orders",
        List.of("producer", "sendWithTags"),
        Set.of("order"),
        Set.of(),
        true);

    final AppProcessingPolicy policy =
        MAPPER.readValue(
            """
            {
              "version": 2,
              "principal": "orders-svc",
              "service": "OrdersService",
              "components": ["kafka-streams"],
              "sources": [],
              "aggregations": [],
              "sinks": [],
              "egressPaths": [],
              "graph": {"nodes": [], "edges": []}
            }
            """,
            AppProcessingPolicy.class);

    ProcessingPolicyEnricher.enrich(policy);

    assertTrue(policy.getComponents().contains("kafka-producer"));
    assertTrue(
        policy.getEgressPaths().stream().anyMatch(path -> "orders".equals(path.getTopic())));
    assertTrue(
        policy.getSinks().stream().anyMatch(sink -> "orders".equals(sink.getTopic())));
    assertTrue(
        policy.getGraph().getEdges().stream().anyMatch(edge -> "writes".equals(edge.getLabel())));
    assertTrue(policy.getRelationalAlgebraAnalysis() != null);
  }
}
