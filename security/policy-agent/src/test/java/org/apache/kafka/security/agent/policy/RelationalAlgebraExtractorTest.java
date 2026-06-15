package org.apache.kafka.security.agent.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.security.agent.AppClientPolicyTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

class RelationalAlgebraExtractorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach
  void resetClientCapture() {
    AppClientPolicyTracker.clearStateForTests();
    EgressProjectionRegistry.clearForTests();
    TopicFieldRegistry.clearForTests();
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
                      "operators": ["filter", "mapValues", "merge"],
                      "callbackProjections": [
                        {"operator":"filter","outputFields":[]},
                        {"operator":"mapValues","outputFields":["orderId","checkType","validationResult"]}
                      ]
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
  void selectionPredicateAppearsInAlgebraExpression() throws Exception {
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
                      "operators": ["filter", "mapValues"],
                      "callbackProjections": [
                        {
                          "operator":"filter",
                          "outputFields":[],
                          "selectionFields":["state","quantity"],
                          "selectionExpression":"state ∧ quantity"
                        },
                        {"operator":"mapValues","outputFields":["orderId","checkType","validationResult"]}
                      ]
                    }
                  ]
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null);
    assertTrue(path.getAlgebraExpression().contains("state ∧ quantity"));
    assertTrue(path.getAlgebraExpression().contains("σ["));
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

  @Test
  void egressProjectionRegistrySanitizesPaymentsPath() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "sources": ["orders"],
                  "egressPaths": [
                    {
                      "topic": "payments",
                      "ingressTopics": ["orders"],
                      "operators": ["filter", "mapValues", "declassifyTags", "to"],
                      "callbackProjections": [
                        {"operator":"filter","outputFields":[]},
                        {"operator":"mapValues","outputFields":["id","status","source"]}
                      ]
                    }
                  ],
                  "graph": {"nodes": [], "edges": []}
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "payments");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.getOutputFields().contains("id"));
    assertFalse(path.getRetainedFields().contains("customerId"));
    assertTrue(path.getDroppedSensitiveFields().contains("customerId"));
  }

  @Test
  void inventoryProcessAfterJoinSanitizesValidationFields() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "inventory-svc",
                  "sources": ["orders", "warehouse-inventory"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": ["selectKey", "filter", "join", "process"],
                      "callbackProjections": [
                        {"operator":"selectKey","outputFields":[]},
                        {"operator":"filter","outputFields":[]},
                        {"operator":"join","outputFields":[]},
                        {"operator":"process","outputFields":["orderId","checkType","validationResult"]}
                      ]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"topic_inventory","kind":"topic","topic":"warehouse-inventory"},
                      {"id":"op_join","kind":"operator","label":"join()"},
                      {"id":"op_process","kind":"operator","label":"process()"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_join","label":"left"},
                      {"from":"topic_inventory","to":"op_join","label":"right"},
                      {"from":"op_join","to":"op_process","label":"input"},
                      {"from":"op_process","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.getAlgebraExpression().contains("process") || path.getAlgebraExpression().contains("π"));
    assertFalse(path.getRetainedFields().contains("customerId"));
    assertTrue(path.getDroppedSensitiveFields().contains("customerId"));
  }

  @Test
  void fallsBackToEgressMetadataWhenGraphMissingIngressScan() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "inventory-svc",
                  "sources": ["orders", "warehouse-inventory"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders", "warehouse-inventory"],
                      "operators": ["selectKey", "filter", "join", "process"],
                      "callbackProjections": [
                        {"operator":"selectKey","outputFields":[]},
                        {"operator":"filter","outputFields":[]},
                        {"operator":"join","outputFields":[]},
                        {"operator":"process","outputFields":["orderId","checkType","validationResult"]}
                      ]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_inventory","kind":"topic","topic":"warehouse-inventory"},
                      {"id":"op_join","kind":"operator","label":"join()"},
                      {"id":"op_process","kind":"operator","label":"process()"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_inventory","to":"op_join","label":"right"},
                      {"from":"op_join","to":"op_process","label":"input"},
                      {"from":"op_process","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(RelationalAlgebraTreeSupport.containsScan(path.getExpressionTree(), "orders"));
    assertTrue(RelationalAlgebraTreeSupport.containsScan(path.getExpressionTree(), "warehouse-inventory"));
    assertTrue(path.getAlgebraExpression().contains("process") || path.getAlgebraExpression().contains("π"));
    assertFalse(path.getRetainedFields().contains("customerId"));
    assertTrue(path.getDroppedSensitiveFields().contains("customerId"));
  }

  @Test
  void callbackProjectionsDriveProcessPiOnJoinPath() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "inventory-svc",
                  "sources": ["orders", "warehouse-inventory"],
                  "egressPaths": [{
                    "topic": "order-validations",
                    "ingressTopics": ["orders", "warehouse-inventory"],
                    "operators": ["selectKey", "filter", "join", "process"],
                    "callbackProjections": [
                      {"operator":"selectKey","outputFields":[]},
                      {"operator":"filter","outputFields":[]},
                      {"operator":"join","outputFields":[]},
                      {"operator":"process","outputFields":["orderId","checkType","validationResult"]}
                    ]
                  }],
                  "graph": {"nodes":[],"edges":[]}
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.getOutputFields().contains("orderId"));
    assertFalse(path.getRetainedFields().contains("customerId"));
    assertTrue(path.getDroppedSensitiveFields().contains("customerId"));
  }

  private static AppProcessingPolicy readPolicy(final String json) throws Exception {
    return MAPPER.readValue(json, AppProcessingPolicy.class);
  }
}
