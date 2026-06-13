package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Extracts relational-algebra plans and field-sanitization metrics from an app processing-policy
 * graph using stream–table duality: KStream/KTable nodes, repartition/changelog internal topics,
 * table-source edges, and state-store materialization.
 */
public final class RelationalAlgebraExtractor {

  private RelationalAlgebraExtractor() {
  }

  public static AppProcessingPolicy.RelationalAlgebraAnalysis analyze(final AppProcessingPolicy policy) {
    final AppProcessingPolicy.RelationalAlgebraAnalysis analysis =
        new AppProcessingPolicy.RelationalAlgebraAnalysis();
    if (policy == null || policy.getGraph() == null) {
      return analysis;
    }
    final List<AppProcessingPolicy.ProcessingPathAnalysis> paths = new ArrayList<>();
    for (final AppProcessingPolicy.EgressPath egress : policy.getEgressPaths()) {
      final String sinkTopic = egress.getTopic();
      if (sinkTopic == null || sinkTopic.isEmpty()) {
        continue;
      }
      final AppProcessingPolicy.RelationalAlgebraExpressionNode tree =
          RelationalAlgebraTreeBuilder.buildForEgress(policy.getGraph(), sinkTopic);
      AppProcessingPolicy.RelationalAlgebraExpressionNode resolved =
          tree != null ? tree : RelationalAlgebraTreeBuilder.buildFromEgressMetadata(policy, egress);
      if (resolved != null && RelationalAlgebraTreeBuilder.scanTopics(resolved).isEmpty()) {
        final AppProcessingPolicy.RelationalAlgebraExpressionNode metadataTree =
            RelationalAlgebraTreeBuilder.buildFromEgressMetadata(policy, egress);
        if (metadataTree != null) {
          resolved = metadataTree;
        }
      }
      if (resolved == null) {
        continue;
      }
      paths.add(buildPathFromTree(sinkTopic, resolved));
    }
    analysis.setProcessingPaths(paths);
    analysis.setTableSourceCount(countTableSources(policy.getGraph()));
    analysis.setRepartitionTopicCount(countInternalTopics(policy.getGraph(), "repartition"));
    analysis.setChangelogTopicCount(countInternalTopics(policy.getGraph(), "changelog"));
    return analysis;
  }

  private static AppProcessingPolicy.ProcessingPathAnalysis buildPathFromTree(
      final String sinkTopic,
      final AppProcessingPolicy.RelationalAlgebraExpressionNode tree) {
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        new AppProcessingPolicy.ProcessingPathAnalysis();
    path.setEgressTopic(sinkTopic);
    path.setExpressionTree(tree);
    path.setAlgebraExpression(RelationalAlgebraTreeSupport.formatTree(tree));

    final Set<String> ingressTopics = RelationalAlgebraTreeBuilder.scanTopics(tree);
    path.setIngressTopics(new ArrayList<>(ingressTopics));
    if (!ingressTopics.isEmpty()) {
      path.setIngressTopic(ingressTopics.iterator().next());
    }

    RelationalAlgebraTreeSupport.collectFlags(tree, path);
    final Set<String> outputFields = RelationalAlgebraTreeSupport.evaluateOutputFields(tree, sinkTopic);
    path.setOutputFields(new ArrayList<>(outputFields));
    path.setSchemaChanged(!ingressTopics.isEmpty() && !ingressTopics.contains(sinkTopic));

    final Set<String> allInputFields = new LinkedHashSet<>();
    final Set<String> allDropped = new LinkedHashSet<>();
    final Set<String> allRetained = new LinkedHashSet<>();
    final Set<String> allDroppedSensitive = new LinkedHashSet<>();
    for (final String ingress : ingressTopics) {
      final Set<String> inputFields = TopicSchemaCatalog.valueFieldsForTopic(ingress);
      allInputFields.addAll(inputFields);
      final Set<String> dropped = new LinkedHashSet<>(inputFields);
      dropped.removeAll(outputFields);
      allDropped.addAll(dropped);
      final Set<String> retained = new LinkedHashSet<>(inputFields);
      retained.retainAll(outputFields);
      allRetained.addAll(retained);
      final Set<String> droppedSensitive = new LinkedHashSet<>(dropped);
      droppedSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
      allDroppedSensitive.addAll(droppedSensitive);
    }
    path.setInputFields(new ArrayList<>(allInputFields));
    path.setDroppedFields(new ArrayList<>(allDropped));
    path.setRetainedFields(new ArrayList<>(allRetained));
    path.setDroppedSensitiveFields(new ArrayList<>(allDroppedSensitive));
    final int inputCount = Math.max(1, allInputFields.size());
    path.setFieldSanitizationRatio((double) allDropped.size() / inputCount);
    path.setSensitiveFieldSanitizationRatio((double) allDroppedSensitive.size() / inputCount);
    return path;
  }

  private static int countTableSources(final ProcessingPolicyGraph graph) {
    int count = 0;
    for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
      if ("table-source".equals(edge.getLabel()) || "table-changelog".equals(edge.getLabel())) {
        count++;
      }
    }
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if ("stream".equals(node.getKind())
          && node.getLabel() != null
          && node.getLabel().contains("KTable")) {
        count++;
      }
    }
    return count;
  }

  private static int countInternalTopics(final ProcessingPolicyGraph graph, final String kind) {
    int count = 0;
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if ("internalTopic".equals(node.getKind()) && kind.equals(node.getInternalTopicKind())) {
        count++;
      }
    }
    return count;
  }

  public static AppProcessingPolicy.ProcessingPathAnalysis pathAnalysisFor(
      final AppProcessingPolicy policy,
      final String ingressTopic,
      final String egressTopic) {
    if (policy == null || policy.getRelationalAlgebraAnalysis() == null) {
      return null;
    }
    for (final AppProcessingPolicy.ProcessingPathAnalysis path :
        policy.getRelationalAlgebraAnalysis().getProcessingPaths()) {
      if (!egressTopic.equals(path.getEgressTopic())) {
        continue;
      }
      if (path.getExpressionTree() != null
          && RelationalAlgebraTreeSupport.containsScan(path.getExpressionTree(), ingressTopic)) {
        return path;
      }
      if (ingressTopic.equals(path.getIngressTopic())) {
        return path;
      }
    }
    return null;
  }

  /** Per-scan lineage for grant verification on tree-structured plans. */
  public static ScanLineage scanLineageFor(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String scanTopic,
      final String sinkTopic) {
    if (path == null || path.getExpressionTree() == null) {
      return null;
    }
    final AppProcessingPolicy.RelationalAlgebraExpressionNode scan =
        RelationalAlgebraTreeSupport.findScan(path.getExpressionTree(), scanTopic);
    if (scan == null) {
      return null;
    }
    final Set<String> scanFields = TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(scanTopic));
    final Set<String> finalFields =
        RelationalAlgebraTreeSupport.evaluateOutputFields(path.getExpressionTree(), sinkTopic);
    final Set<String> retained = new LinkedHashSet<>(scanFields);
    retained.retainAll(finalFields);
    final Set<String> dropped = new LinkedHashSet<>(scanFields);
    dropped.removeAll(finalFields);
    return new ScanLineage(scanTopic, scanFields, finalFields, retained, dropped);
  }

  public record ScanLineage(
      String scanTopic,
      Set<String> scanFields,
      Set<String> finalOutputFields,
      Set<String> retainedFields,
      Set<String> droppedFields) {}
}
