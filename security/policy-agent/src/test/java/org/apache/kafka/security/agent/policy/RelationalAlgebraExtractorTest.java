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
  void projectionExpressionAppearsInAlgebraExpression() throws Exception {
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
                      "operators": ["mapValues"],
                      "callbackProjections": [
                        {
                          "operator":"mapValues",
                          "outputFields":["orderId","checkType","validationResult"],
                          "fieldLineages":[
                            {
                              "outputField":"orderId",
                              "valueType":"STRING",
                              "sourceFields":["id"],
                              "expression":"orderId := id",
                              "sanitizationKind":"DERIVED"
                            }
                          ]
                        }
                      ]
                    }
                  ]
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null);
    assertTrue(path.getAlgebraExpression().contains("orderId := id"));
    assertTrue(path.getAlgebraExpression().contains("π["));
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
  void splitMergeGraphRetainsBranchingTopologyWithMapValuesProjection() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "fraud-svc",
                  "sources": ["orders"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": ["filter", "split", "mapValues", "merge"],
                      "declassifyTags": ["order"],
                      "callbackProjections": [
                        {"operator":"filter","selectionFields":["state","quantity"]},
                        {"operator":"mapValues","outputFields":["orderId","checkType","validationResult"]}
                      ]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_filter","kind":"operator","label":"filter()"},
                      {"id":"op_split","kind":"operator","label":"split()"},
                      {"id":"branch_above","kind":"stream","label":"KStream"},
                      {"id":"branch_below","kind":"stream","label":"KStream"},
                      {"id":"op_map_above","kind":"operator","label":"mapValues()"},
                      {"id":"op_map_below","kind":"operator","label":"mapValues()"},
                      {"id":"op_merge","kind":"operator","label":"merge()"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_filter","label":"source"},
                      {"from":"op_filter","to":"op_split","label":"input"},
                      {"from":"op_split","to":"branch_above","label":"branch[0]"},
                      {"from":"op_split","to":"branch_below","label":"branch[1]"},
                      {"from":"branch_above","to":"op_map_above","label":"input"},
                      {"from":"branch_below","to":"op_map_below","label":"input"},
                      {"from":"op_map_above","to":"op_merge","label":"left"},
                      {"from":"op_map_below","to":"op_merge","label":"right"},
                      {"from":"op_merge","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(RelationalAlgebraTreeSupport.hasBranchingTopology(path.getExpressionTree()));
    assertTrue(path.getAlgebraExpression().contains("∪"));
    assertTrue(path.getOutputFields().contains("orderId"));
    assertFalse(path.getOutputFields().contains("customerId"));
    assertTrue(path.getDroppedSensitiveFields().contains("customerId"));
  }

  @Test
  void runtimeCapturedFraudEgressSynthesizesSplitMergeBranches() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "fraud-svc",
                  "sources": ["orders"],
                  "aggregations": ["aggregate", "groupBy", "merge", "split", "windowedBy"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": ["mapValues", "merge"],
                      "declassifyTags": ["order"],
                      "callbackProjections": [
                        {"operator":"mapValues","outputFields":["buildPropertiesFromConfigFile"]},
                        {"operator":"merge","outputFields":[]}
                      ]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_split","kind":"operator","label":"split()"},
                      {"id":"op_merge","kind":"operator","label":"merge()"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_merge","label":"input"},
                      {"from":"op_merge","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.getAlgebraExpression().contains("∪"));
    assertTrue(path.getOutputFields().contains("orderId"));
    assertFalse(path.getOutputFields().contains("customerId"));
  }

  @Test
  void fraudSplitMergeMetadataDocumentsWindowAndBranchSelections() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "fraud-svc",
                  "sources": ["orders"],
                  "aggregations": ["aggregate", "groupBy", "windowedBy"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": [
                        "filter", "groupBy", "windowedBy", "aggregate",
                        "split", "mapValues", "merge"
                      ],
                      "declassifyTags": ["order"],
                      "callbackProjections": [
                        {"operator":"mapValues","outputFields":["orderId","checkType","validationResult"]},
                        {"operator":"merge","outputFields":[]}
                      ]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_split","kind":"operator","label":"split()"},
                      {"id":"op_merge","kind":"operator","label":"merge()"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_merge","label":"input"},
                      {"from":"op_merge","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && path.getExpressionTree() != null);
    final String expr = path.getAlgebraExpression();
    assertTrue(expr.contains("ω") || RelationalAlgebraTreeSupport.treeContainsOperator(
        path.getExpressionTree(), "windowedby"));
    assertTrue(expr.contains("γ_g") || RelationalAlgebraTreeSupport.treeContainsOperator(
        path.getExpressionTree(), "groupby"));
    assertTrue(expr.contains("session order total"));
    assertEquals(1, RelationalAlgebraTreeBuilder.scanTopics(path.getExpressionTree()).size());
    assertTrue(path.isAggregated());
  }

  @Test
  void fraudMergeGraphPrefersMetadataMapValuesProjection() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "fraud-svc",
                  "sources": ["orders"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": ["mapValues", "merge"],
                      "declassifyTags": ["order"],
                      "callbackProjections": [
                        {"operator":"mapValues","outputFields":["orderId","checkType","validationResult"]},
                        {"operator":"merge","outputFields":[]}
                      ]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_merge","kind":"operator","label":"merge()"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_merge","label":"input"},
                      {"from":"op_merge","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(
        path.getAlgebraExpression().contains("π")
            || path.getAlgebraExpression().toLowerCase(java.util.Locale.ROOT).contains("mapvalues"));
    assertTrue(path.getOutputFields().contains("orderId"));
    assertFalse(path.getOutputFields().contains("customerId"));
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

  @Test
  void stockServiceManifestBuildsProjectionTreeAndConsumesOrderTopic() throws Exception {
    System.setProperty("policy.app.principal", "stock-svc");
    AppClientPolicyTracker.recordConsumerSubscribe(
        java.util.List.of(JugPipelineProjections.TOPIC_ORDER));
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "stock-svc",
                  "service": "StockService",
                  "sources": [],
                  "egressPaths": [],
                  "graph": {"nodes": [], "edges": []}
                }
                """));
    final Set<String> consumed =
        ProcessingPolicyGraphHelper.consumedTopics(
            policy, Set.of(JugPipelineProjections.TOPIC_ORDER));
    assertFalse(consumed.isEmpty());
    assertTrue(consumed.contains(JugPipelineProjections.TOPIC_ORDER));

    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(
            policy,
            JugPipelineProjections.TOPIC_ORDER,
            JugPipelineProjections.TOPIC_STOCK_CHECK);
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.getAlgebraExpression().contains("Scan(" + JugPipelineProjections.TOPIC_ORDER + ")"));
    assertTrue(path.getOutputFields().contains("customerId"));
    assertFalse(path.getOutputFields().contains("price"));
    assertTrue(path.getDroppedFields().contains("price"));
  }

  @Test
  void validationServiceCardRemoveConsumesStockCheckTopic() throws Exception {
    System.setProperty("policy.app.principal", "validation-svc");
    AppClientPolicyTracker.recordConsumerSubscribe(
        java.util.List.of(JugPipelineProjections.TOPIC_STOCK_CHECK));
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "principal": "validation-svc",
                  "service": "ValidationService",
                  "sources": [],
                  "egressPaths": [],
                  "graph": {"nodes": [], "edges": []}
                }
                """));
    final Set<String> consumed =
        ProcessingPolicyGraphHelper.consumedTopics(
            policy,
            Set.of(
                JugPipelineProjections.TOPIC_ORDER,
                JugPipelineProjections.TOPIC_STOCK_CHECK));
    assertTrue(consumed.contains(JugPipelineProjections.TOPIC_STOCK_CHECK));

    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(
            policy,
            JugPipelineProjections.TOPIC_STOCK_CHECK,
            JugPipelineProjections.TOPIC_VALIDATION);
    assertTrue(path != null && path.getExpressionTree() != null);
    assertTrue(path.getAlgebraExpression().contains("Scan(" + JugPipelineProjections.TOPIC_STOCK_CHECK + ")"));
    assertFalse(path.getOutputFields().contains("cardNumber"));
  }

  @Test
  void taintReportMarksUnsanitizedDerivedAndDroppedFields() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            readPolicy(
                """
                {
                  "version": 2,
                  "sources": ["orders"],
                  "egressPaths": [{
                    "topic": "order-validations",
                    "ingressTopics": ["orders"],
                    "operators": ["mapValues"],
                    "callbackProjections": [
                      {
                        "operator":"mapValues",
                        "outputFields":["orderId","checkType","validationResult"],
                        "fieldLineages":[
                          {
                            "outputField":"orderId",
                            "valueType":"STRING",
                            "sourceFields":["id"],
                            "expression":"id",
                            "sanitizationKind":"PASSTHROUGH"
                          },
                          {
                            "outputField":"checkType",
                            "valueType":"STRING",
                            "sourceFields":[],
                            "expression":"FRAUD_CHECK",
                            "sanitizationKind":"CONSTANT"
                          },
                          {
                            "outputField":"validationResult",
                            "valueType":"DOUBLE",
                            "sourceFields":["price", "quantity"],
                            "expression":"price * quantity",
                            "sanitizationKind":"DERIVED"
                          }
                        ]
                      }
                    ]
                  }]
                }
                """));
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null && !path.getSourceFieldTaint().isEmpty());
    assertTrue(path.getSourceFieldCount() >= 6);
    assertTrue(path.getUnsanitizedSourceFieldCount() >= 1);
    assertTrue(
        path.getSourceFieldTaint().stream()
            .anyMatch(
                status ->
                    "price".equals(status.getSourceField())
                        && !status.isSanitized()
                        && status.getStatus().contains("UNSANITIZED")));
    assertTrue(
        path.getSourceFieldTaint().stream()
            .anyMatch(
                status ->
                    "customerId".equals(status.getSourceField())
                        && status.isSanitized()
                        && "DROPPED".equals(status.getStatus())));
  }

  private static AppProcessingPolicy readPolicy(final String json) throws Exception {
    return MAPPER.readValue(json, AppProcessingPolicy.class);
  }
}
