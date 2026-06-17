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
      final AppProcessingPolicy.RelationalAlgebraExpressionNode graphTree =
          RelationalAlgebraTreeBuilder.buildForEgress(policy, sinkTopic);
      AppProcessingPolicy.RelationalAlgebraExpressionNode resolved = graphTree;
      if (resolved == null) {
        resolved = RelationalAlgebraTreeBuilder.buildFromEgressMetadata(policy, egress);
      }
      final Set<String> expectedIngress =
          ProcessingPolicyGraphHelper.ingressTopicsForEgressPath(policy, egress);
      final Set<String> scanTopics =
          resolved == null ? Set.of() : RelationalAlgebraTreeBuilder.scanTopics(resolved);
      if (resolved != null
          && !expectedIngress.isEmpty()
          && !scanTopics.containsAll(expectedIngress)) {
        final AppProcessingPolicy.RelationalAlgebraExpressionNode metadataTree =
            RelationalAlgebraTreeBuilder.buildFromEgressMetadata(policy, egress);
        if (metadataTree != null) {
          resolved = metadataTree;
        }
      } else if (resolved != null && scanTopics.isEmpty()) {
        final AppProcessingPolicy.RelationalAlgebraExpressionNode metadataTree =
            RelationalAlgebraTreeBuilder.buildFromEgressMetadata(policy, egress);
        if (metadataTree != null) {
          resolved = metadataTree;
        }
      }
      if (resolved == null) {
        continue;
      }
      if (RelationalAlgebraTreeSupport.mergeTopologyCollapsed(resolved, policy, egress)) {
        List<String> operators =
            PolicyManifestRegistry.manifestOperatorsFor(policy.getPrincipal(), sinkTopic);
        if (operators.isEmpty()) {
          operators = egress.getOperators() == null ? List.of() : egress.getOperators();
        }
        List<AppProcessingPolicy.OperatorCallbackProjection> callbacks =
            egress.getCallbackProjections() == null ? List.of() : egress.getCallbackProjections();
        final List<AppProcessingPolicy.OperatorCallbackProjection> manifestCallbacks =
            PolicyManifestRegistry.manifestCallbackProjectionsFor(policy.getPrincipal(), sinkTopic);
        if (!manifestCallbacks.isEmpty()) {
          callbacks = manifestCallbacks;
        }
        final Set<String> ingressTopics =
            ProcessingPolicyGraphHelper.ingressTopicsForEgressPath(policy, egress);
        final AppProcessingPolicy.RelationalAlgebraExpressionNode splitMerge =
            RelationalAlgebraTreeBuilder.buildSplitMergeFromEgressMetadata(
                new ArrayList<>(ingressTopics),
                sinkTopic,
                operators,
                RelationalAlgebraTreeBuilder.alignCallbacksToOperators(operators, callbacks));
        if (splitMerge != null) {
          resolved = splitMerge;
        }
      }
      final AppProcessingPolicy.RelationalAlgebraExpressionNode metadataTree =
          RelationalAlgebraTreeBuilder.buildFromEgressMetadata(policy, egress);
      if (metadataTree != null && graphMissingSanitizationOperators(resolved, egress)) {
        if (RelationalAlgebraTreeSupport.hasBranchingTopology(resolved)) {
          RelationalAlgebraTreeBuilder.overlaySanitizationFromMetadata(resolved, egress, metadataTree);
        } else {
          resolved = metadataTree;
        }
      } else if (metadataTree != null && shouldPreferMetadataTree(policy, egress, resolved, metadataTree)) {
        if (RelationalAlgebraTreeSupport.hasBranchingTopology(resolved)) {
          RelationalAlgebraTreeBuilder.overlaySanitizationFromMetadata(resolved, egress, metadataTree);
        } else {
          resolved = metadataTree;
        }
      }
      paths.add(buildPathFromTree(policy, sinkTopic, resolved));
    }
    analysis.setProcessingPaths(paths);
    analysis.setTableSourceCount(countTableSources(policy.getGraph()));
    analysis.setRepartitionTopicCount(countInternalTopics(policy.getGraph(), "repartition"));
    analysis.setChangelogTopicCount(countInternalTopics(policy.getGraph(), "changelog"));
    return analysis;
  }

  private static boolean graphMissingSanitizationOperators(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode graphTree,
      final AppProcessingPolicy.EgressPath egress) {
    if (graphTree == null || egress == null || egress.getCallbackProjections() == null) {
      return false;
    }
    for (final AppProcessingPolicy.OperatorCallbackProjection callback : egress.getCallbackProjections()) {
      if (callback.getOperator() == null) {
        continue;
      }
      final boolean documentsSanitization =
          (callback.getOutputFields() != null && !callback.getOutputFields().isEmpty())
              || (callback.getSelectionFields() != null && !callback.getSelectionFields().isEmpty())
              || (callback.getSelectionExpression() != null && !callback.getSelectionExpression().isEmpty());
      if (!documentsSanitization) {
        continue;
      }
      final String op = RelationalAlgebraTreeSupport.normalizeOp(callback.getOperator() + "()");
      if (!RelationalAlgebraTreeSupport.treeContainsOperator(graphTree, op)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Prefer egress-metadata trees when the graph-only plan retains order-sensitive fields but
   * callbacks/declassify on the egress path document a narrowing projection (e.g. Fraud mapValues).
   */
  private static boolean shouldPreferMetadataTree(
      final AppProcessingPolicy policy,
      final AppProcessingPolicy.EgressPath egress,
      final AppProcessingPolicy.RelationalAlgebraExpressionNode graphTree,
      final AppProcessingPolicy.RelationalAlgebraExpressionNode metadataTree) {
    if (egress == null || egress.getTopic() == null || graphTree == null || metadataTree == null) {
      return false;
    }
    final String sinkTopic = egress.getTopic();
    final Set<String> graphOut = RelationalAlgebraTreeSupport.evaluateOutputFields(graphTree, sinkTopic);
    final Set<String> metaOut = RelationalAlgebraTreeSupport.evaluateOutputFields(metadataTree, sinkTopic);
    final Set<String> graphOrderSensitive = new LinkedHashSet<>(graphOut);
    graphOrderSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
    final Set<String> metaOrderSensitive = new LinkedHashSet<>(metaOut);
    metaOrderSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
    if (!graphOrderSensitive.isEmpty() && metaOrderSensitive.isEmpty() && !metaOut.isEmpty()) {
      return true;
    }
    if (egress.getDeclassifyTags() != null
        && !egress.getDeclassifyTags().isEmpty()
        && egressDocumentsSanitizationOperators(egress)
        && !graphOrderSensitive.isEmpty()
        && metaOrderSensitive.isEmpty()) {
      return true;
    }
    return false;
  }

  private static boolean egressDocumentsSanitizationOperators(
      final AppProcessingPolicy.EgressPath egress) {
    if (egress.getCallbackProjections() != null) {
      for (final AppProcessingPolicy.OperatorCallbackProjection callback : egress.getCallbackProjections()) {
        if (callback.getOutputFields() != null && !callback.getOutputFields().isEmpty()) {
          return true;
        }
      }
    }
    if (egress.getOperators() == null) {
      return false;
    }
    for (final String operator : egress.getOperators()) {
      final String normalized = operator == null ? "" : operator.toLowerCase(java.util.Locale.ROOT);
      if (normalized.contains("mapvalues")
          || normalized.contains("map")
          || normalized.contains("filter")
          || normalized.contains("process")
          || normalized.contains("aggregate")) {
        return true;
      }
    }
    return false;
  }

  private static AppProcessingPolicy.ProcessingPathAnalysis buildPathFromTree(
      final AppProcessingPolicy policy,
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
    enrichStreamTableMetadata(path, policy);
    final Set<String> outputFields = RelationalAlgebraTreeSupport.evaluateOutputFields(tree, sinkTopic);
    path.setOutputFields(new ArrayList<>(outputFields));
    path.setSchemaChanged(!ingressTopics.isEmpty() && !ingressTopics.contains(sinkTopic));

    final Set<String> allInputFields = new LinkedHashSet<>();
    final Set<String> allDropped = new LinkedHashSet<>();
    final Set<String> allRetained = new LinkedHashSet<>();
    final Set<String> allDroppedSensitive = new LinkedHashSet<>();
    for (final String ingress : ingressTopics) {
      final Set<String> inputFields = TopicSchemaResolver.valueFieldsForTopic(ingress);
      allInputFields.addAll(inputFields);
      final Set<String> dropped = new LinkedHashSet<>(inputFields);
      dropped.removeAll(outputFields);
      allDropped.addAll(dropped);
      final Set<String> retained = new LinkedHashSet<>(inputFields);
      retained.retainAll(outputFields);
      allRetained.addAll(retained);
      final Set<String> droppedSensitive = new LinkedHashSet<>(dropped);
      droppedSensitive.retainAll(TopicSchemaResolver.sensitiveOrderFields());
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

  private static void enrichStreamTableMetadata(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final AppProcessingPolicy policy) {
    if (policy == null || policy.getGraph() == null || path.getIngressTopic() == null) {
      return;
    }
    final List<StreamTablePathExtractor.Step> streamTableSteps =
        ProcessingPolicyGraphHelper.streamTableStepsOnSanitizedPath(
            policy.getGraph(), path.getIngressTopic(), path.getEgressTopic());
    if (streamTableSteps.isEmpty()) {
      return;
    }
    final List<AppProcessingPolicy.RelationalAlgebraStep> steps = new ArrayList<>();
    for (final StreamTablePathExtractor.Step streamStep : streamTableSteps) {
      steps.add(toRelationalStep(streamStep));
      switch (streamStep.getKind()) {
        case REPARTITION -> path.setRepartitionInvolved(true);
        case CHANGELOG -> path.setChangelogInvolved(true);
        case TABLE_FROM_CHANGELOG, TABLE_STATE -> path.setTableInvolved(true);
        case STATE_STORE -> {
          final List<String> stores = new ArrayList<>(path.getStateStores());
          if (streamStep.getStoreName() != null && !stores.contains(streamStep.getStoreName())) {
            stores.add(streamStep.getStoreName());
          }
          path.setStateStores(stores);
        }
        default -> {
        }
      }
    }
    path.setSteps(steps);
    if (path.isAggregated() && path.isChangelogInvolved()) {
      path.setTableMaterializedFromAggregate(true);
    }
  }

  private static AppProcessingPolicy.RelationalAlgebraStep toRelationalStep(
      final StreamTablePathExtractor.Step streamStep) {
    final AppProcessingPolicy.RelationalAlgebraStep step = new AppProcessingPolicy.RelationalAlgebraStep();
    step.setOperator(streamStep.getName());
    step.setDescription(streamStep.getEdgeLabel());
    step.setInternalTopic(streamStep.getTopic());
    step.setStoreName(streamStep.getStoreName());
    step.setStreamTableKind(
        streamStep.getKind() == null ? null : streamStep.getKind().name());
    return switch (streamStep.getKind()) {
      case REPARTITION -> {
        step.setAlgebraSymbol("ρ");
        yield step;
      }
      case CHANGELOG -> {
        step.setAlgebraSymbol("λ_c");
        yield step;
      }
      case TABLE_FROM_CHANGELOG, TABLE_STATE -> {
        step.setAlgebraSymbol("λ_t");
        yield step;
      }
      case STATE_STORE -> {
        step.setAlgebraSymbol("ω");
        yield step;
      }
      default -> step;
    };
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
    final Set<String> scanFields = TopicSchemaCatalog.copyFields(TopicSchemaResolver.valueFieldsForTopic(scanTopic));
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
