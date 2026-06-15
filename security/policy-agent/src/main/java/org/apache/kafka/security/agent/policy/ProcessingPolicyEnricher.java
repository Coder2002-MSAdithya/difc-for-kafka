package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enriches agent-exported policies with runtime app-client capture, optional manifest supplements,
 * ingress derivation, and relational-algebra analysis before signing.
 */
public final class ProcessingPolicyEnricher {

  private static final String MANIFEST_SUPPLEMENT_PROP = "policy.manifest.supplement";

  private ProcessingPolicyEnricher() {
  }

  public static AppProcessingPolicy enrich(final AppProcessingPolicy policy) {
    if (policy == null) {
      return null;
    }
    org.apache.kafka.security.agent.AppClientPolicyTracker.mergeInto(policy);
    if (policy.getPrincipal() != null && manifestSupplementEnabled()) {
      final ProcessingPolicyDocument manifest = manifestFor(policy.getPrincipal());
      if (manifest != null) {
        if (shouldApplyFullManifest(policy)) {
          merge(policy, manifest);
        } else {
          supplementManifest(policy, manifest);
          mergeManifestGraphGaps(policy, manifest);
        }
      }
    }
    applyEnrichment(policy);
    return policy;
  }

  private static boolean shouldApplyFullManifest(final AppProcessingPolicy policy) {
    return policy.getGraph().getNodes().isEmpty();
  }

  private static boolean manifestSupplementEnabled() {
    return Boolean.parseBoolean(System.getProperty(MANIFEST_SUPPLEMENT_PROP, "true"));
  }

  private static void supplementManifest(
      final AppProcessingPolicy policy,
      final ProcessingPolicyDocument manifest) {
    final Set<String> existingEgress = new LinkedHashSet<>();
    for (final AppProcessingPolicy.EgressPath path : policy.getEgressPaths()) {
      if (path.getTopic() != null) {
        existingEgress.add(path.getTopic());
      }
    }
    final ProcessingPolicyDocument filtered = new ProcessingPolicyDocument();
    filtered.setApplicationId(manifest.getApplicationId());
    filtered.setPrincipal(manifest.getPrincipal());
    for (final ProcessingPolicyDocument.SinkBinding binding : manifest.getSinkBindings()) {
      if (binding.getEgressTopic() != null && !existingEgress.contains(binding.getEgressTopic())) {
        filtered.getSinkBindings().add(binding);
      }
    }
    filtered.setTableSources(new ArrayList<>(manifest.getTableSources()));
    if (filtered.getSinkBindings().isEmpty() && filtered.getTableSources().isEmpty()) {
      return;
    }
    merge(policy, filtered);
  }

  /**
   * Adds manifest graph/table edges when runtime capture documented egress but not a sanitizing graph path.
   */
  private static void mergeManifestGraphGaps(
      final AppProcessingPolicy policy,
      final ProcessingPolicyDocument manifest) {
    final ProcessingPolicyGraph graph = policy.getGraph();
    for (final String tableSource : manifest.getTableSources()) {
      mergeManifestTableSource(graph, manifest.getApplicationId(), tableSource);
    }
    for (final ProcessingPolicyDocument.SinkBinding binding : manifest.getSinkBindings()) {
      if (needsManifestGraphMerge(policy, binding)) {
        mergeManifestGraph(graph, binding, manifest.getApplicationId());
      }
    }
    policy.setGraph(graph);
  }

  private static boolean needsManifestGraphMerge(
      final AppProcessingPolicy policy,
      final ProcessingPolicyDocument.SinkBinding binding) {
    if (binding.getIngressTopics().isEmpty()) {
      return !hasWritesEdgeToEgress(policy.getGraph(), binding.getEgressTopic());
    }
    for (final String ingress : binding.getIngressTopics()) {
      if (ProcessingPolicyGraphHelper.documentsSanitizedPath(
          policy, ingress, binding.getEgressTopic())) {
        return false;
      }
    }
    return true;
  }

  private static boolean hasWritesEdgeToEgress(
      final ProcessingPolicyGraph graph,
      final String egressTopic) {
    final String egressNodeId = topicNodeId(graph, egressTopic);
    if (egressNodeId == null) {
      return false;
    }
    for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
      if (egressNodeId.equals(edge.getTo()) && "writes".equals(edge.getLabel())) {
        return true;
      }
    }
    return false;
  }

  private static String topicNodeId(final ProcessingPolicyGraph graph, final String topic) {
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if ("topic".equals(node.getKind()) && topic.equals(node.getTopic())) {
        return node.getId();
      }
    }
    return null;
  }

  private static void applyEnrichment(final AppProcessingPolicy policy) {
    ProcessingPolicyGraphHelper.enrichEgressPathsFromGraph(policy);
    ProcessingPolicyGraphHelper.enrichEgressPathsFromSources(policy);
    policy.setAggregationAnalysis(ProcessingPolicyGraphHelper.computeAggregationAnalysis(policy));
    policy.setRelationalAlgebraAnalysis(RelationalAlgebraExtractor.analyze(policy));
  }

  private static ProcessingPolicyDocument manifestFor(final String principal) {
    return switch (principal) {
      case "orders-svc" -> PolicyManifestRegistry.ordersProducer();
      case "fraud-svc" -> PolicyManifestRegistry.fraudValidator();
      case "inventory-svc" -> PolicyManifestRegistry.inventoryValidator();
      case "order-details-svc" -> PolicyManifestRegistry.orderDetailsValidator();
      case "email-svc" -> PolicyManifestRegistry.emailConsumer();
      case "validations-agg-svc" -> PolicyManifestRegistry.validationsAggregator();
      default -> null;
    };
  }

  private static AppProcessingPolicy merge(
      final AppProcessingPolicy agent,
      final ProcessingPolicyDocument manifest) {
    final Set<String> components = new LinkedHashSet<>(agent.getComponents());
    components.add("kafka-producer");
    components.add("rest");

    final Set<String> sources = new LinkedHashSet<>(agent.getSources());
    final Set<String> aggregations = new LinkedHashSet<>(agent.getAggregations());
    final Map<String, AppProcessingPolicy.EgressPath> egressByTopic = new LinkedHashMap<>();
    for (final AppProcessingPolicy.EgressPath existing : agent.getEgressPaths()) {
      mergeEgressPath(egressByTopic, existing);
    }
    final List<AppProcessingPolicy.SinkPolicy> sinks = new ArrayList<>(agent.getSinks());
    final ProcessingPolicyGraph graph = agent.getGraph();

    for (final ProcessingPolicyDocument.SinkBinding binding : manifest.getSinkBindings()) {
      sources.addAll(binding.getIngressTopics());

      final AppProcessingPolicy.EgressPath path = new AppProcessingPolicy.EgressPath();
      path.setTopic(binding.getEgressTopic());
      path.setIngressTopics(new ArrayList<>(binding.getIngressTopics()));
      path.setOperators(new ArrayList<>(binding.getOperators()));
      path.setDeclassifyTags(new ArrayList<>(binding.getRemovedTags()));
      path.setAddTags(new ArrayList<>(binding.getAddedTags()));
      mergeEgressPath(egressByTopic, path);

      final AppProcessingPolicy.SinkPolicy sink = new AppProcessingPolicy.SinkPolicy();
      sink.setTopic(binding.getEgressTopic());
      sink.setDeclassifyTags(new ArrayList<>(binding.getRemovedTags()));
      sink.setAddTags(new ArrayList<>(binding.getAddedTags()));
      sinks.add(sink);

      aggregations.addAll(binding.aggregatorOperators());
      mergeManifestGraph(graph, binding, manifest.getApplicationId());
    }

    for (final String tableSource : manifest.getTableSources()) {
      mergeManifestTableSource(graph, manifest.getApplicationId(), tableSource);
    }

    agent.setComponents(new ArrayList<>(components));
    agent.setSources(new ArrayList<>(sources));
    agent.setAggregations(new ArrayList<>(aggregations));
    agent.setEgressPaths(new ArrayList<>(egressByTopic.values()));
    agent.setSinks(sinks);
    agent.setGraph(graph);
    return agent;
  }

  private static void mergeEgressPath(
      final Map<String, AppProcessingPolicy.EgressPath> egressByTopic,
      final AppProcessingPolicy.EgressPath path) {
    if (path.getTopic() == null || path.getTopic().isEmpty()) {
      return;
    }
    final AppProcessingPolicy.EgressPath merged =
        egressByTopic.computeIfAbsent(path.getTopic(), t -> {
          final AppProcessingPolicy.EgressPath created = new AppProcessingPolicy.EgressPath();
          created.setTopic(t);
          return created;
        });
    merged.setIngressTopics(union(merged.getIngressTopics(), path.getIngressTopics()));
    merged.setOperators(unionOperators(merged.getOperators(), path.getOperators()));
    merged.setDeclassifyTags(union(merged.getDeclassifyTags(), path.getDeclassifyTags()));
    merged.setAddTags(union(merged.getAddTags(), path.getAddTags()));
    merged.setCallbackProjections(mergeCallbackProjections(merged, path));
  }

  private static List<String> unionOperators(final List<String> left, final List<String> right) {
    final List<String> merged = new ArrayList<>(left == null ? List.of() : left);
    if (right != null) {
      merged.addAll(right);
    }
    return merged;
  }

  private static List<AppProcessingPolicy.OperatorCallbackProjection> mergeCallbackProjections(
      final AppProcessingPolicy.EgressPath left,
      final AppProcessingPolicy.EgressPath right) {
    if (right.getCallbackProjections().isEmpty()) {
      return left.getCallbackProjections() == null
          ? new ArrayList<>()
          : new ArrayList<>(left.getCallbackProjections());
    }
    final List<AppProcessingPolicy.OperatorCallbackProjection> merged = new ArrayList<>();
    if (left.getCallbackProjections() != null) {
      merged.addAll(left.getCallbackProjections());
    }
    merged.addAll(right.getCallbackProjections());
    return merged;
  }

  private static void mergeManifestGraph(
      final ProcessingPolicyGraph graph,
      final ProcessingPolicyDocument.SinkBinding binding,
      final String applicationId) {
    final String componentId = "comp_" + applicationId.replaceAll("[^A-Za-z0-9_]", "_");
    ensureGraphNode(graph, componentId, "component", applicationId + " (REST/producer)", null);

    final String egressTopicId = ensureTopicGraphNode(graph, binding.getEgressTopic());

    for (final String ingress : binding.getIngressTopics()) {
      final String ingressId = ensureTopicGraphNode(graph, ingress);
      ensureGraphEdge(graph, ingressId, componentId, "ingress");
    }

    if (binding.getIngressTopics().size() == 1) {
      addManifestOperatorPath(
          graph,
          binding.getIngressTopics().get(0),
          binding.getEgressTopic(),
          binding.getOperators(),
          componentId);
    } else if (binding.getIngressTopics().size() > 1) {
      final String joinId = componentId + "_join";
      final String aggregateId = componentId + "_aggregate";
      ensureGraphNode(graph, joinId, "operator", "join()", null);
      ensureGraphNode(graph, aggregateId, "operator", "aggregate()", null);
      for (final String ingress : binding.getIngressTopics()) {
        final String ingressId = ensureTopicGraphNode(graph, ingress);
        if (ingress.equals(binding.getEgressTopic())) {
          ensureGraphEdge(graph, ingressId, joinId, "left");
        } else {
          ensureGraphEdge(graph, ingressId, aggregateId, "source");
          ensureGraphEdge(graph, aggregateId, joinId, "right");
        }
      }
      ensureGraphEdge(graph, joinId, egressTopicId, "writes");
    }
  }

  private static void mergeManifestTableSource(
      final ProcessingPolicyGraph graph,
      final String applicationId,
      final String tableTopic) {
    final String componentId = "comp_" + applicationId.replaceAll("[^A-Za-z0-9_]", "_");
    final String tableNodeId = componentId + "_ktable_" + tableTopic.replaceAll("[^A-Za-z0-9_\\-]", "_");
    ensureGraphNode(graph, componentId, "component", applicationId + " (REST/producer)", null);
    ensureGraphNode(graph, tableNodeId, "stream", "KTable[aggregated-state]", null);
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if (tableNodeId.equals(node.getId())) {
        node.setTableRole("aggregated-state");
        break;
      }
    }
    final String topicId = ensureTopicGraphNode(graph, tableTopic);
    ensureGraphEdge(graph, topicId, tableNodeId, "table-changelog");
    ensureGraphEdge(graph, topicId, componentId, "ingress");
  }

  private static void addManifestOperatorPath(
      final ProcessingPolicyGraph graph,
      final String ingressTopic,
      final String egressTopic,
      final List<String> operators,
      final String pathPrefix) {
    final List<String> relational = new ArrayList<>();
    for (final String operator : operators) {
      if (ProcessingPolicyGraphHelper.isRelationalSanitizationOperator(operator)) {
        relational.add(operator);
      }
    }
    if (relational.isEmpty()) {
      return;
    }
    String previous = ensureTopicGraphNode(graph, ingressTopic);
    for (int i = 0; i < relational.size(); i++) {
      final String opId = pathPrefix + "_op_" + i;
      ensureGraphNode(graph, opId, "operator", relational.get(i) + "()", null);
      ensureGraphEdge(graph, previous, opId, i == 0 ? "source" : "input");
      previous = opId;
    }
    ensureGraphEdge(graph, previous, ensureTopicGraphNode(graph, egressTopic), "writes");
  }

  private static String ensureTopicGraphNode(final ProcessingPolicyGraph graph, final String topic) {
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if ("topic".equals(node.getKind()) && topic.equals(node.getTopic())) {
        return node.getId();
      }
    }
    final String id = "topic_" + topic.replaceAll("[^A-Za-z0-9_\\-]", "_");
    ensureGraphNode(graph, id, "topic", "topic:" + topic, topic);
    return id;
  }

  private static void ensureGraphNode(
      final ProcessingPolicyGraph graph,
      final String id,
      final String kind,
      final String label,
      final String topic) {
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if (id.equals(node.getId())) {
        return;
      }
    }
    final ProcessingPolicyGraph.GraphNode node = new ProcessingPolicyGraph.GraphNode();
    node.setId(id);
    node.setKind(kind);
    node.setLabel(label);
    node.setTopic(topic);
    graph.getNodes().add(node);
  }

  private static void ensureGraphEdge(
      final ProcessingPolicyGraph graph,
      final String from,
      final String to,
      final String label) {
    for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
      if (from.equals(edge.getFrom()) && to.equals(edge.getTo()) && label.equals(edge.getLabel())) {
        return;
      }
    }
    final ProcessingPolicyGraph.GraphEdge edge = new ProcessingPolicyGraph.GraphEdge();
    edge.setFrom(from);
    edge.setTo(to);
    edge.setLabel(label);
    graph.getEdges().add(edge);
  }

  private static List<String> union(final List<String> left, final List<String> right) {
    final LinkedHashSet<String> merged = new LinkedHashSet<>(left);
    merged.addAll(right);
    return new ArrayList<>(merged);
  }
}
