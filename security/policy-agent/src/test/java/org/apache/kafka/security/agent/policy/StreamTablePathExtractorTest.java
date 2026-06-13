package org.apache.kafka.security.agent.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.kafka.security.agent.AppClientPolicyTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamTablePathExtractorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach
  void resetClientCapture() {
    AppClientPolicyTracker.clearStateForTests();
  }

  @Test
  void extractsRepartitionChangelogAndTableSteps() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            MAPPER.readValue(
                """
                {
                  "version": 2,
                  "sources": ["orders"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": ["selectKey", "filter", "mapValues"]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_select","kind":"operator","label":"selectKey()"},
                      {"id":"internal_repartition","kind":"internalTopic","topic":"InventoryService-orders-repartition","internalTopicKind":"repartition"},
                      {"id":"op_filter","kind":"operator","label":"filter()"},
                      {"id":"op_map","kind":"operator","label":"mapValues()"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_select","label":"source"},
                      {"from":"op_select","to":"internal_repartition","label":"repartition-write"},
                      {"from":"internal_repartition","to":"op_filter","label":"repartition-read"},
                      {"from":"op_filter","to":"op_map","label":"output"},
                      {"from":"op_map","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """,
                AppProcessingPolicy.class));

    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null);
    assertTrue(path.isRepartitionInvolved());
    assertTrue(path.getAlgebraExpression().contains("ρ"));
    assertTrue(
        path.getSteps().stream().anyMatch(s -> "REPARTITION".equals(s.getStreamTableKind())));
  }

  @Test
  void inventoryManifestModelsTableFromChangelog() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            MAPPER.readValue(
                """
                {
                  "version": 2,
                  "principal": "inventory-svc",
                  "service": "InventoryService",
                  "sources": ["orders", "warehouse-inventory"],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": ["selectKey", "filter", "join", "mapValues"]
                    }
                  ],
                  "graph": {"nodes": [], "edges": []}
                }
                """,
                AppProcessingPolicy.class));

    assertTrue(
        policy.getGraph().getEdges().stream()
            .anyMatch(e -> "table-changelog".equals(e.getLabel())));
    final AppProcessingPolicy.RelationalAlgebraAnalysis analysis = policy.getRelationalAlgebraAnalysis();
    assertTrue(analysis.getTableSourceCount() >= 1);
  }

  @Test
  void aggregatePathModelsChangelogAndStateStore() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            MAPPER.readValue(
                """
                {
                  "version": 2,
                  "sources": ["orders"],
                  "egressPaths": [
                    {"topic": "order-validations", "ingressTopics": ["orders"], "operators": ["aggregate", "mapValues"]}
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_aggregate","kind":"operator","label":"aggregate()"},
                      {"id":"internal_changelog","kind":"internalTopic","topic":"FraudService-aggregate-changelog","internalTopicKind":"changelog"},
                      {"id":"op_map","kind":"operator","label":"mapValues()"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_aggregate","label":"source"},
                      {"from":"op_aggregate","to":"internal_changelog","label":"changelog-write"},
                      {"from":"internal_changelog","to":"op_map","label":"changelog-read"},
                      {"from":"op_map","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """,
                AppProcessingPolicy.class));

    final AppProcessingPolicy.ProcessingPathAnalysis path =
        RelationalAlgebraExtractor.pathAnalysisFor(policy, "orders", "order-validations");
    assertTrue(path != null);
    assertTrue(path.isChangelogInvolved());
    assertTrue(path.isTableMaterializedFromAggregate());
    assertTrue(path.getAlgebraExpression().contains("λ_c"));
  }

  @Test
  void stateStoreMaterializationStepOnPath() throws Exception {
    final AppProcessingPolicy policy =
        ProcessingPolicyEnricher.enrich(
            MAPPER.readValue(
                """
                {
                  "version": 2,
                  "sources": ["orders"],
                  "egressPaths": [
                    {"topic": "order-validations", "ingressTopics": ["orders"], "operators": ["process"]}
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_process","kind":"operator","label":"process()"},
                      {"id":"store_reserved","kind":"stateStore","storeName":"reserved-stock","label":"state-store:reserved-stock"},
                      {"id":"topic_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_process","label":"source"},
                      {"from":"op_process","to":"store_reserved","label":"materializes"},
                      {"from":"op_process","to":"topic_validations","label":"writes"}
                    ]
                  }
                }
                """,
                AppProcessingPolicy.class));

    final List<StreamTablePathExtractor.Step> steps =
        ProcessingPolicyGraphHelper.streamTableStepsOnSanitizedPath(
            policy.getGraph(), "orders", "order-validations");
    assertTrue(steps.stream().anyMatch(s -> s.getKind() == StreamTablePathExtractor.StepKind.STATE_STORE));
  }
}
