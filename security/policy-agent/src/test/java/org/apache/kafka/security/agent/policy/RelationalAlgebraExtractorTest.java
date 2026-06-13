package org.apache.kafka.security.agent.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.security.agent.AppClientPolicyTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalAlgebraExtractorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach
  void resetClientCapture() {
    AppClientPolicyTracker.clearStateForTests();
  }

  @Test
  void ordersToValidationsDropsSensitiveFields() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "sources": ["orders"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": ["filter", "mapValues", "merge"]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_filter","kind":"operator","label":"filter()"},
                      {"id":"op_map","kind":"operator","label":"mapValues()"},
                      {"id":"topic_order_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_filter","label":"source"},
                      {"from":"op_filter","to":"op_map","label":"output"},
                      {"from":"op_map","to":"topic_order_validations","label":"writes"}
                    ]
                  }
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.getAlgebraExpression().contains("Scan(orders)"));
    assertTrue(path.isSchemaChanged());
    assertTrue(path.isFiltered());
    assertEquals(5, path.getDroppedSensitiveFields().size());
    assertTrue(path.getSensitiveFieldSanitizationRatio() > 0.8);
    assertTrue(path.getDroppedSensitiveFields().contains("customerId"));
  }

  @Test
  void ordersRepublicationViaJoinRetainsSensitiveFields() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "sources": ["orders"],
                  "egressPaths": [
                    {"topic": "orders", "ingressTopics": ["orders"], "operators": ["join"]}
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_join","kind":"operator","label":"join()"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_join","label":"left"},
                      {"from":"op_join","to":"topic_orders","label":"writes"}
                    ]
                  }
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "orders");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.isJoined());
    assertTrue(path.getAlgebraExpression().contains("⋈"));
    assertTrue(path.getRetainedFields().contains("customerId"));
  }

  @Test
  void validationsAggregateBeforeJoinSanitizesValidationFields() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "validations-agg-svc",
                  "service": "ValidationsAggregatorService",
                  "sources": ["orders", "order-validations"],
                  "egressPaths": [
                    {
                      "topic": "orders",
                      "ingressTopics": ["order-validations", "orders"],
                      "operators": ["aggregate", "join", "merge"]
                    }
                  ],
                  "graph": {"nodes": [], "edges": []}
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "order-validations", "orders");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.isAggregated() || path.isJoined());
    assertTrue(path.getIngressTopics().contains("order-validations"));
    assertTrue(path.getIngressTopics().contains("orders"));
    assertTrue(path.getAlgebraExpression().contains("Scan(order-validations)"));
    assertTrue(path.getAlgebraExpression().contains("Scan(orders)"));
    final AppProcessingPolicy.ProcessingPathAnalysis ordersPath =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "orders");
    assertTrue(ordersPath != null && ordersPath.isJoined());
  }

  private static AppProcessingPolicy readPolicy(final String json) throws Exception {
    return MAPPER.readValue(json, AppProcessingPolicy.class);
  }
}
