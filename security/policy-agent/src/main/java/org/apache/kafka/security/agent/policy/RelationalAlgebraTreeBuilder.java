package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds a nested relational-algebra expression tree per egress sink by walking the processing
 * graph backward from the sink (joins become multi-child nodes with multiple {@code Scan} roots).
 */
public final class RelationalAlgebraTreeBuilder {

  private RelationalAlgebraTreeBuilder() {
  }

  public static AppProcessingPolicy.RelationalAlgebraExpressionNode buildForEgress(
      final AppProcessingPolicy policy,
      final String sinkTopic) {
    if (policy == null || policy.getGraph() == null || sinkTopic == null || sinkTopic.isEmpty()) {
      return null;
    }
    final AppProcessingPolicy.EgressPath egress = egressPathForTopic(policy, sinkTopic);
    return buildFromGraph(policy.getGraph(), sinkTopic, egress);
  }

  /** @deprecated use {@link #buildForEgress(AppProcessingPolicy, String)} */
  @Deprecated
  public static AppProcessingPolicy.RelationalAlgebraExpressionNode buildForEgress(
      final ProcessingPolicyGraph graph,
      final String sinkTopic) {
    if (graph == null || sinkTopic == null || sinkTopic.isEmpty()) {
      return null;
    }
    return buildFromGraph(graph, sinkTopic, null);
  }

  private static AppProcessingPolicy.EgressPath egressPathForTopic(
      final AppProcessingPolicy policy,
      final String sinkTopic) {
    if (policy == null || policy.getEgressPaths() == null) {
      return null;
    }
    for (final AppProcessingPolicy.EgressPath egress : policy.getEgressPaths()) {
      if (sinkTopic.equals(egress.getTopic())) {
        return egress;
      }
    }
    return null;
  }

  public static AppProcessingPolicy.RelationalAlgebraExpressionNode buildFromEgressMetadata(
      final AppProcessingPolicy policy,
      final AppProcessingPolicy.EgressPath egress) {
    if (policy == null || egress == null || egress.getTopic() == null) {
      return null;
    }
    final Set<String> ingressTopics =
        ProcessingPolicyGraphHelper.ingressTopicsForEgressPath(policy, egress);
    if (ingressTopics.isEmpty()) {
      return null;
    }
    final List<String> operators = egress.getOperators() == null ? List.of() : egress.getOperators();
    final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks =
        alignCallbacksToOperators(
            operators,
            egress.getCallbackProjections() == null ? List.of() : egress.getCallbackProjections());
    final AppProcessingPolicy.RelationalAlgebraExpressionNode splitMerge =
        buildSplitMergeFromEgressMetadata(
            new ArrayList<>(ingressTopics), egress.getTopic(), operators, callbacks);
    if (splitMerge != null) {
      return splitMerge;
    }
    if (ingressTopics.size() == 1) {
      return buildLinearTree(ingressTopics.iterator().next(), egress.getTopic(), operators, callbacks);
    }
    return buildJoinTree(new ArrayList<>(ingressTopics), egress.getTopic(), operators, callbacks);
  }

  /**
   * When egress metadata documents split → mapValues* → merge, synthesize a multi-branch RA tree
   * even if branch stream nodes are not wired to split in the captured DSL graph.
   */
  public static AppProcessingPolicy.RelationalAlgebraExpressionNode buildSplitMergeFromEgressMetadata(
      final List<String> ingressTopics,
      final String sinkTopic,
      final List<String> operators,
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks) {
    if (ingressTopics.size() != 1 || operators == null || operators.isEmpty()) {
      return null;
    }
    final List<String> normalized = new ArrayList<>();
    for (final String raw : operators) {
      normalized.add(RelationalAlgebraTreeSupport.normalizeOp(raw + "()"));
    }
    int splitIdx = -1;
    int mergeIdx = -1;
    for (int i = 0; i < normalized.size(); i++) {
      if (splitIdx < 0 && ("split".equals(normalized.get(i)) || "branch".equals(normalized.get(i)))) {
        splitIdx = i;
      }
      if (splitIdx >= 0 && "merge".equals(normalized.get(i))) {
        mergeIdx = i;
        break;
      }
    }
    if (splitIdx < 0 || mergeIdx <= splitIdx) {
      return null;
    }

    final List<String> prefixRaw = operators.subList(0, splitIdx);
    final List<AppProcessingPolicy.OperatorCallbackProjection> prefixCallbacks =
        prefixCallbacks(callbacks, operators, 0, splitIdx);
    final AppProcessingPolicy.RelationalAlgebraExpressionNode prefixBody =
        buildOperatorChain(ingressTopics.get(0), sinkTopic, prefixRaw, prefixCallbacks);

    final AppProcessingPolicy.RelationalAlgebraExpressionNode merge =
        RelationalAlgebraTreeSupport.operatorNode("merge", "∪", "union");

    AppProcessingPolicy.OperatorCallbackProjection mapCallback = null;
    int mapOperatorIndex = -1;
    for (int i = splitIdx + 1; i < mergeIdx; i++) {
      final String op = normalized.get(i);
      if ("mapvalues".equals(op) || "process".equals(op)) {
        mapOperatorIndex = i;
        if (callbacks != null && i < callbacks.size()) {
          mapCallback = callbacks.get(i);
        }
        break;
      }
    }

    for (int branch = 0; branch < 2; branch++) {
      AppProcessingPolicy.RelationalAlgebraExpressionNode branchRoot = prefixBody;
      if (mapOperatorIndex >= 0) {
        final String op = normalized.get(mapOperatorIndex);
        final AppProcessingPolicy.RelationalAlgebraExpressionNode projection =
            RelationalAlgebraTreeSupport.operatorNode(
                op,
                RelationalAlgebraTreeSupport.algebraSymbol(op),
                RelationalAlgebraTreeSupport.operatorDescription(op, sinkTopic));
        projection.getChildren().add(branchRoot);
        if (mapCallback != null) {
          applyCallbackAnalysis(projection, mapCallback);
        } else {
          applyCallbackOutputFields(projection, mapOperatorIndex, callbacks);
        }
        RelationalAlgebraTreeSupport.annotateOutputFields(projection, sinkTopic);
        branchRoot = projection;
      }
      merge.getChildren().add(branchRoot);
    }

    applyCallbackOutputFields(merge, mergeIdx, callbacks);
    RelationalAlgebraTreeSupport.annotateOutputFields(merge, sinkTopic);

    AppProcessingPolicy.RelationalAlgebraExpressionNode body = merge;
    body = wrapPostJoinOperators(
        body, sinkTopic, normalized.subList(mergeIdx + 1, normalized.size()), callbacks, mergeIdx + 1);

    final AppProcessingPolicy.RelationalAlgebraExpressionNode sink =
        RelationalAlgebraTreeSupport.sinkNode(sinkTopic);
    sink.getChildren().add(body);
    RelationalAlgebraTreeSupport.annotateOutputFields(sink, sinkTopic);
    return sink;
  }

  private static List<AppProcessingPolicy.OperatorCallbackProjection> prefixCallbacks(
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks,
      final List<String> operators,
      final int fromInclusive,
      final int toExclusive) {
    if (callbacks == null || callbacks.isEmpty()) {
      return List.of();
    }
    final List<AppProcessingPolicy.OperatorCallbackProjection> aligned =
        alignCallbacksToOperators(operators, callbacks);
    if (fromInclusive >= toExclusive || fromInclusive >= aligned.size()) {
      return List.of();
    }
    return new ArrayList<>(aligned.subList(fromInclusive, Math.min(toExclusive, aligned.size())));
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildFromGraph(
      final ProcessingPolicyGraph graph,
      final String sinkTopic,
      final AppProcessingPolicy.EgressPath egress) {
    final String sinkId = topicNodeId(graph, sinkTopic);
    if (sinkId == null) {
      return null;
    }
    final Map<String, List<String>> reverse = reverseAdjacency(graph);
    final List<String> writers = new ArrayList<>();
    for (final String pred : reverse.getOrDefault(sinkId, List.of())) {
      final ProcessingPolicyGraph.GraphNode node = ProcessingPolicyGraphHelper.nodeById(graph, pred);
      if (node != null && "operator".equals(node.getKind())) {
        writers.add(pred);
      }
    }
    if (writers.isEmpty()) {
      return null;
    }

    final AppProcessingPolicy.RelationalAlgebraExpressionNode sink =
        RelationalAlgebraTreeSupport.sinkNode(sinkTopic);
    final Set<String> visiting = new LinkedHashSet<>();
    final AppProcessingPolicy.RelationalAlgebraExpressionNode body =
        buildUpstream(writers.get(0), graph, reverse, sinkTopic, visiting, egress);
    if (body == null) {
      return null;
    }
    sink.getChildren().add(body);
    RelationalAlgebraTreeSupport.annotateOutputFields(sink, sinkTopic);
    return sink;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildLinearTree(
      final String ingressTopic,
      final String sinkTopic,
      final List<String> operators,
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode body =
        buildOperatorChain(ingressTopic, sinkTopic, operators, callbacks);
    final AppProcessingPolicy.RelationalAlgebraExpressionNode sink =
        RelationalAlgebraTreeSupport.sinkNode(sinkTopic);
    sink.getChildren().add(body);
    RelationalAlgebraTreeSupport.annotateOutputFields(sink, sinkTopic);
    return sink;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildOperatorChain(
      final String ingressTopic,
      final String sinkTopic,
      final List<String> operators,
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks) {
    AppProcessingPolicy.RelationalAlgebraExpressionNode current =
        RelationalAlgebraTreeSupport.scanNode(ingressTopic);
    for (int i = operators.size() - 1; i >= 0; i--) {
      final String op = RelationalAlgebraTreeSupport.normalizeOp(operators.get(i) + "()");
      if (RelationalAlgebraTreeSupport.isPassthroughOp(op)) {
        continue;
      }
      final AppProcessingPolicy.RelationalAlgebraExpressionNode unary =
          RelationalAlgebraTreeSupport.operatorNode(
              op,
              RelationalAlgebraTreeSupport.algebraSymbol(op),
              RelationalAlgebraTreeSupport.operatorDescription(op, sinkTopic));
      unary.getChildren().add(current);
      applyCallbackOutputFields(unary, i, callbacks);
      RelationalAlgebraTreeSupport.annotateOutputFields(unary, sinkTopic);
      current = unary;
    }
    return current;
  }

  private static void applyCallbackOutputFields(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final int operatorIndex,
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks) {
    if (callbacks == null || operatorIndex < 0 || operatorIndex >= callbacks.size()) {
      return;
    }
    applyCallbackAnalysis(node, callbacks.get(operatorIndex));
  }

  static void applyCallbackAnalysis(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final AppProcessingPolicy.OperatorCallbackProjection callback) {
    if (node == null || callback == null) {
      return;
    }
    if (callback.getOutputFields() != null && !callback.getOutputFields().isEmpty()) {
      node.setOutputFields(new ArrayList<>(callback.getOutputFields()));
    }
    if (callback.getSelectionFields() != null && !callback.getSelectionFields().isEmpty()) {
      node.setSelectionFields(new ArrayList<>(callback.getSelectionFields()));
    }
    if (callback.getSelectionExpression() != null && !callback.getSelectionExpression().isEmpty()) {
      node.setSelectionExpression(callback.getSelectionExpression());
    }
    if (callback.getKeyFields() != null && !callback.getKeyFields().isEmpty()) {
      node.setKeyFields(new ArrayList<>(callback.getKeyFields()));
    }
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildJoinTree(
      final List<String> ingressTopics,
      final String sinkTopic,
      final List<String> operators,
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks) {
    final List<String> normalized = new ArrayList<>();
    for (final String raw : operators) {
      normalized.add(RelationalAlgebraTreeSupport.normalizeOp(raw + "()"));
    }
    final int joinIdx = normalized.indexOf("join");
    final List<String> preJoinOps =
        joinIdx > 0 ? normalized.subList(0, joinIdx) : List.of();
    final List<String> postJoinOps =
        joinIdx >= 0 && joinIdx + 1 < normalized.size()
            ? normalized.subList(joinIdx + 1, normalized.size())
            : List.of();

    final AppProcessingPolicy.RelationalAlgebraExpressionNode join =
        RelationalAlgebraTreeSupport.operatorNode("join", "⋈", "stream–table join");
    for (int i = 0; i < ingressTopics.size(); i++) {
      final String ingress = ingressTopics.get(i);
      final AppProcessingPolicy.RelationalAlgebraExpressionNode branch;
      if (i == 0 && !preJoinOps.isEmpty()) {
        branch = buildOperatorChain(ingress, sinkTopic, stripOpSuffix(preJoinOps), prefixCallbacks(callbacks, 0, joinIdx));
      } else {
        branch = RelationalAlgebraTreeSupport.scanNode(ingress);
      }
      join.getChildren().add(branch);
    }
    applyCallbackOutputFields(join, joinIdx, callbacks);
    RelationalAlgebraTreeSupport.annotateOutputFields(join, sinkTopic);

    AppProcessingPolicy.RelationalAlgebraExpressionNode body = join;
    body = wrapPostJoinOperators(body, sinkTopic, postJoinOps, callbacks, joinIdx + 1);

    final AppProcessingPolicy.RelationalAlgebraExpressionNode sink =
        RelationalAlgebraTreeSupport.sinkNode(sinkTopic);
    sink.getChildren().add(body);
    RelationalAlgebraTreeSupport.annotateOutputFields(sink, sinkTopic);
    return sink;
  }

  private static List<String> stripOpSuffix(final List<String> ops) {
    final List<String> stripped = new ArrayList<>();
    for (final String op : ops) {
      stripped.add(op.endsWith("()") ? op.substring(0, op.length() - 2) : op);
    }
    return stripped;
  }

  private static List<AppProcessingPolicy.OperatorCallbackProjection> prefixCallbacks(
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks,
      final int fromInclusive,
      final int toExclusive) {
    if (callbacks == null || callbacks.isEmpty() || fromInclusive >= toExclusive) {
      return List.of();
    }
    final int end = Math.min(toExclusive, callbacks.size());
    if (fromInclusive >= end) {
      return List.of();
    }
    return new ArrayList<>(callbacks.subList(fromInclusive, end));
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode wrapPostJoinOperators(
      AppProcessingPolicy.RelationalAlgebraExpressionNode body,
      final String sinkTopic,
      final List<String> postJoinOps,
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks,
      final int callbackOffset) {
    int callbackIndex = callbackOffset;
    for (int i = postJoinOps.size() - 1; i >= 0; i--) {
      final String op = postJoinOps.get(i);
      if (RelationalAlgebraTreeSupport.isPassthroughOp(op)) {
        callbackIndex++;
        continue;
      }
      final AppProcessingPolicy.RelationalAlgebraExpressionNode unary =
          RelationalAlgebraTreeSupport.operatorNode(
              op,
              RelationalAlgebraTreeSupport.algebraSymbol(op),
              RelationalAlgebraTreeSupport.operatorDescription(op, sinkTopic));
      unary.getChildren().add(body);
      applyCallbackOutputFields(unary, callbackIndex, callbacks);
      RelationalAlgebraTreeSupport.annotateOutputFields(unary, sinkTopic);
      body = unary;
      callbackIndex++;
    }
    return body;
  }

  public static Set<String> scanTopics(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode root) {
    final Set<String> topics = new LinkedHashSet<>();
    collectScanTopics(root, topics);
    return topics;
  }

  private static void collectScanTopics(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final Set<String> topics) {
    if (node == null) {
      return;
    }
    if ("scan".equals(node.getKind()) && node.getTopic() != null) {
      topics.add(node.getTopic());
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      collectScanTopics(child, topics);
    }
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildUpstream(
      final String nodeId,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting,
      final AppProcessingPolicy.EgressPath egress) {
    if (nodeId == null || !visiting.add(nodeId)) {
      return null;
    }
    final ProcessingPolicyGraph.GraphNode node = ProcessingPolicyGraphHelper.nodeById(graph, nodeId);
    if (node == null) {
      visiting.remove(nodeId);
      return null;
    }

    AppProcessingPolicy.RelationalAlgebraExpressionNode result = null;
    switch (node.getKind()) {
      case "topic" -> {
        if (node.getTopic() != null && !isInternalTopic(node.getTopic())) {
          result = RelationalAlgebraTreeSupport.scanNode(node.getTopic());
        }
      }
      case "internalTopic" -> result = buildThroughInternal(nodeId, graph, reverse, sinkTopic, visiting, egress);
      case "stream" -> result = buildThroughStream(nodeId, graph, reverse, sinkTopic, visiting, egress);
      case "operator" -> result = buildOperator(nodeId, node, graph, reverse, sinkTopic, visiting, egress);
      default -> result = buildThroughStream(nodeId, graph, reverse, sinkTopic, visiting, egress);
    }
    visiting.remove(nodeId);
    return result;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildThroughStream(
      final String nodeId,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting,
      final AppProcessingPolicy.EgressPath egress) {
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode up =
          buildUpstream(pred, graph, reverse, sinkTopic, visiting, egress);
      if (up != null) {
        return up;
      }
    }
    return null;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildThroughInternal(
      final String nodeId,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting,
      final AppProcessingPolicy.EgressPath egress) {
    final ProcessingPolicyGraph.GraphNode internalNode =
        ProcessingPolicyGraphHelper.nodeById(graph, nodeId);
    final String internalKind =
        internalNode == null ? "repartition" : internalNode.getInternalTopicKind();
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode up =
          buildUpstream(pred, graph, reverse, sinkTopic, visiting, egress);
      if (up != null) {
        final AppProcessingPolicy.RelationalAlgebraExpressionNode wrapper;
        if ("changelog".equals(internalKind)) {
          wrapper =
              RelationalAlgebraTreeSupport.operatorNode("changelog", "λ_c", "changelog materialization");
        } else {
          wrapper = RelationalAlgebraTreeSupport.operatorNode("repartition", "ρ", "repartition");
        }
        wrapper.getChildren().add(up);
        RelationalAlgebraTreeSupport.annotateOutputFields(wrapper, sinkTopic);
        return wrapper;
      }
    }
    return null;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildOperator(
      final String nodeId,
      final ProcessingPolicyGraph.GraphNode node,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting,
      final AppProcessingPolicy.EgressPath egress) {
    final String op = RelationalAlgebraTreeSupport.normalizeOp(node.getLabel());
    if (RelationalAlgebraTreeSupport.isJoinOp(op)) {
      return buildJoin(nodeId, op, graph, reverse, sinkTopic, visiting, egress);
    }
    if (RelationalAlgebraTreeSupport.isPassthroughOp(op)) {
      for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
        final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
            buildUpstream(pred, graph, reverse, sinkTopic, visiting, egress);
        if (child != null) {
          return child;
        }
      }
      return null;
    }
    if ("merge".equals(op)) {
      return buildMerge(nodeId, graph, reverse, sinkTopic, visiting, egress);
    }
    if ("split".equals(op) || "branch".equals(op)) {
      return buildSplit(nodeId, op, graph, reverse, sinkTopic, visiting, egress);
    }

    final AppProcessingPolicy.RelationalAlgebraExpressionNode unary =
        RelationalAlgebraTreeSupport.operatorNode(
            op,
            RelationalAlgebraTreeSupport.algebraSymbol(op),
            RelationalAlgebraTreeSupport.operatorDescription(op, sinkTopic));
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
          buildUpstream(pred, graph, reverse, sinkTopic, visiting, egress);
      if (child != null) {
        unary.getChildren().add(child);
        break;
      }
    }
    applyEgressCallback(egress, op, unary);
    RelationalAlgebraTreeSupport.annotateOutputFields(unary, sinkTopic);
    return unary;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildJoin(
      final String nodeId,
      final String op,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting,
      final AppProcessingPolicy.EgressPath egress) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode join =
        RelationalAlgebraTreeSupport.operatorNode(op, "⋈", "stream–table join");
    final List<String> leftBranch = new ArrayList<>();
    final List<String> rightBranch = new ArrayList<>();
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final String edgeLabel = edgeLabel(graph, pred, nodeId);
      if ("right".equals(edgeLabel)) {
        rightBranch.add(pred);
      } else {
        leftBranch.add(pred);
      }
    }
    if (leftBranch.isEmpty() && rightBranch.isEmpty()) {
      for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
        leftBranch.add(pred);
      }
    }
    for (final String pred : leftBranch) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
          buildUpstream(pred, graph, reverse, sinkTopic, new LinkedHashSet<>(visiting), egress);
      if (child != null) {
        join.getChildren().add(child);
      }
    }
    for (final String pred : rightBranch) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
          buildUpstream(pred, graph, reverse, sinkTopic, new LinkedHashSet<>(visiting), egress);
      if (child != null) {
        join.getChildren().add(child);
      }
    }
    if (join.getChildren().isEmpty()) {
      return null;
    }
    applyEgressCallback(egress, op, join);
    RelationalAlgebraTreeSupport.annotateOutputFields(join, sinkTopic);
    return join;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildSplit(
      final String nodeId,
      final String op,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting,
      final AppProcessingPolicy.EgressPath egress) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode split =
        RelationalAlgebraTreeSupport.operatorNode(op, "σ∪", "split branches");
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
          buildUpstream(pred, graph, reverse, sinkTopic, visiting, egress);
      if (child != null) {
        split.getChildren().add(child);
        break;
      }
    }
    if (split.getChildren().isEmpty()) {
      return null;
    }
    applyEgressCallback(egress, op, split);
    RelationalAlgebraTreeSupport.annotateOutputFields(split, sinkTopic);
    return split;
  }

  /**
   * Re-apply egress callback projections onto an existing graph tree and copy narrowed output
   * fields from a metadata tree without replacing branching topology.
   */
  public static void overlaySanitizationFromMetadata(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode graphTree,
      final AppProcessingPolicy.EgressPath egress,
      final AppProcessingPolicy.RelationalAlgebraExpressionNode metadataTree) {
    if (graphTree == null || egress == null) {
      return;
    }
    if (egress.getCallbackProjections() != null) {
      for (final AppProcessingPolicy.OperatorCallbackProjection callback : egress.getCallbackProjections()) {
        overlayCallbackOnMatchingOperators(graphTree, callback);
      }
    }
    if (metadataTree != null) {
      overlayOutputFieldsFromMetadata(graphTree, metadataTree);
    }
    if (egress.getTopic() != null) {
      RelationalAlgebraTreeSupport.annotateOutputFields(graphTree, egress.getTopic());
    }
  }

  private static void overlayCallbackOnMatchingOperators(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final AppProcessingPolicy.OperatorCallbackProjection callback) {
    if (node == null || callback == null || callback.getOperator() == null) {
      return;
    }
    if ("operator".equals(node.getKind())) {
      final String normalized = RelationalAlgebraTreeSupport.normalizeOp(node.getTopic() + "()");
      final String callbackOp = RelationalAlgebraTreeSupport.normalizeOp(callback.getOperator() + "()");
      if (normalized.equals(callbackOp)) {
        applyCallbackAnalysis(node, callback);
      }
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      overlayCallbackOnMatchingOperators(child, callback);
    }
  }

  private static void overlayOutputFieldsFromMetadata(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode graphNode,
      final AppProcessingPolicy.RelationalAlgebraExpressionNode metadataNode) {
    if (graphNode == null || metadataNode == null) {
      return;
    }
    if ("operator".equals(graphNode.getKind())
        && "operator".equals(metadataNode.getKind())
        && graphNode.getTopic() != null
        && graphNode.getTopic().equals(metadataNode.getTopic())
        && graphNode.getOutputFields().isEmpty()
        && !metadataNode.getOutputFields().isEmpty()) {
      graphNode.setOutputFields(new ArrayList<>(metadataNode.getOutputFields()));
    }
    if (!metadataNode.getSelectionExpression().isEmpty() && graphNode.getSelectionExpression().isEmpty()) {
      graphNode.setSelectionExpression(metadataNode.getSelectionExpression());
    }
    if (graphNode.getSelectionFields().isEmpty() && !metadataNode.getSelectionFields().isEmpty()) {
      graphNode.setSelectionFields(new ArrayList<>(metadataNode.getSelectionFields()));
    }
    if (graphNode.getKeyFields().isEmpty() && !metadataNode.getKeyFields().isEmpty()) {
      graphNode.setKeyFields(new ArrayList<>(metadataNode.getKeyFields()));
    }
    final int childCount = Math.min(graphNode.getChildren().size(), metadataNode.getChildren().size());
    for (int i = 0; i < childCount; i++) {
      overlayOutputFieldsFromMetadata(graphNode.getChildren().get(i), metadataNode.getChildren().get(i));
    }
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildMerge(
      final String nodeId,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting,
      final AppProcessingPolicy.EgressPath egress) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode merge =
        RelationalAlgebraTreeSupport.operatorNode("merge", "∪", "union");
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
          buildUpstream(pred, graph, reverse, sinkTopic, new LinkedHashSet<>(visiting), egress);
      if (child != null) {
        merge.getChildren().add(child);
      }
    }
    if (merge.getChildren().isEmpty()) {
      return null;
    }
    applyEgressCallback(egress, "merge", merge);
    RelationalAlgebraTreeSupport.annotateOutputFields(merge, sinkTopic);
    return merge;
  }

  private static void applyEgressCallback(
      final AppProcessingPolicy.EgressPath egress,
      final String op,
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node) {
    if (egress == null || egress.getCallbackProjections() == null || node == null) {
      return;
    }
    final String normalized = RelationalAlgebraTreeSupport.normalizeOp(op + "()");
    for (final AppProcessingPolicy.OperatorCallbackProjection callback : egress.getCallbackProjections()) {
      if (callback.getOperator() == null) {
        continue;
      }
      if (normalized.equals(RelationalAlgebraTreeSupport.normalizeOp(callback.getOperator() + "()"))) {
        applyCallbackAnalysis(node, callback);
        return;
      }
    }
  }

  private static String edgeLabel(
      final ProcessingPolicyGraph graph,
      final String from,
      final String to) {
    final ProcessingPolicyGraph.GraphEdge edge =
        ProcessingPolicyGraphHelper.edgeBetween(graph, from, to);
    return edge == null || edge.getLabel() == null ? "" : edge.getLabel();
  }

  private static boolean isInternalTopic(final String topic) {
    final String upper = topic.toUpperCase();
    return upper.contains("KSTREAM") || upper.contains("REPARTITION");
  }

  private static Map<String, List<String>> reverseAdjacency(final ProcessingPolicyGraph graph) {
    final Map<String, List<String>> reverse = new java.util.LinkedHashMap<>();
    for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
      reverse.computeIfAbsent(edge.getTo(), k -> new ArrayList<>()).add(edge.getFrom());
    }
    return reverse;
  }

  private static String topicNodeId(final ProcessingPolicyGraph graph, final String topic) {
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if ("topic".equals(node.getKind()) && topic.equals(node.getTopic())) {
        return node.getId();
      }
    }
    return null;
  }

  static List<AppProcessingPolicy.OperatorCallbackProjection> alignCallbacksToOperators(
      final List<String> operators,
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks) {
    if (operators == null || operators.isEmpty()) {
      return List.of();
    }
    final List<AppProcessingPolicy.OperatorCallbackProjection> aligned = new ArrayList<>();
    int searchFrom = 0;
    for (final String operator : operators) {
      final String normalized = RelationalAlgebraTreeSupport.normalizeOp(operator + "()");
      if (RelationalAlgebraTreeSupport.isPassthroughOp(normalized)) {
        continue;
      }
      AppProcessingPolicy.OperatorCallbackProjection matched = null;
      int matchedIndex = -1;
      for (int i = searchFrom; i < callbacks.size(); i++) {
        final AppProcessingPolicy.OperatorCallbackProjection candidate = callbacks.get(i);
        if (operator.equals(candidate.getOperator())) {
          matched = candidate;
          matchedIndex = i;
        }
      }
      if (matched != null) {
        searchFrom = matchedIndex + 1;
      }
      if (matched != null) {
        aligned.add(matched);
      } else {
        final AppProcessingPolicy.OperatorCallbackProjection empty =
            new AppProcessingPolicy.OperatorCallbackProjection();
        empty.setOperator(operator);
        empty.setOutputFields(new ArrayList<>());
        empty.setSelectionFields(new ArrayList<>());
        empty.setSelectionExpression("");
        empty.setKeyFields(new ArrayList<>());
        aligned.add(empty);
      }
    }
    return aligned;
  }
}
