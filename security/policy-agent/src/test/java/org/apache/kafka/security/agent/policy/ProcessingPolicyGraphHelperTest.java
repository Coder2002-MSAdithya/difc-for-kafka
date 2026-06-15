package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessingPolicyGraphHelperTest {

  @Test
  void ingressLinksSameTopicRepublicationWhenEgressHasProcessingOperators() {
    final AppProcessingPolicy policy = new AppProcessingPolicy();
    policy.setSources(List.of("order-validations", "orders"));
    final AppProcessingPolicy.EgressPath egress = new AppProcessingPolicy.EgressPath();
    egress.setTopic("orders");
    egress.setOperators(List.of("join", "merge", "aggregate", "filter"));
    policy.setEgressPaths(List.of(egress));

    ProcessingPolicyGraphHelper.enrichEgressPathsFromSources(policy);

    assertTrue(
        policy.getEgressPaths().get(0).getIngressTopics().contains("orders"),
        "orders→orders republication must link consumed grantor topic orders");
    assertTrue(policy.getEgressPaths().get(0).getIngressTopics().contains("order-validations"));
  }

  @Test
  void ingressOmitsSameTopicSourceWithoutDocumentedProcessing() {
    final AppProcessingPolicy policy = new AppProcessingPolicy();
    policy.setSources(List.of("orders"));
    final AppProcessingPolicy.EgressPath egress = new AppProcessingPolicy.EgressPath();
    egress.setTopic("orders");
    egress.setOperators(List.of());
    policy.setEgressPaths(List.of(egress));

    ProcessingPolicyGraphHelper.enrichEgressPathsFromSources(policy);

    assertFalse(
        policy.getEgressPaths().get(0).getIngressTopics().contains("orders"),
        "bare same-topic source without processing operators should not link");
  }

  @Test
  void egressPathsProcessingTopicsIncludesSameTopicRepublication() {
    final AppProcessingPolicy policy = new AppProcessingPolicy();
    policy.setSources(List.of("orders", "order-validations"));
    final AppProcessingPolicy.EgressPath egress = new AppProcessingPolicy.EgressPath();
    egress.setTopic("orders");
    egress.setOperators(List.of("join", "merge"));
    policy.setEgressPaths(List.of(egress));

    assertFalse(
        ProcessingPolicyGraphHelper.egressPathsProcessingTopics(policy, java.util.Set.of("orders"))
            .isEmpty());
  }
}
