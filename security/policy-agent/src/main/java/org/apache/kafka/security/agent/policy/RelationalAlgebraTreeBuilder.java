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
      final ProcessingPolicyGraph graph,
      final String sinkTopic) {
    if (graph == null || sinkTopic == null || sinkTopic.isEmpty()) {
      return null;
    }
    final AppProcessingPolicy.RelationalAlgebraExpressionNode fromGraph =
        buildFromGraph(graph, sinkTopic);
    if (fromGraph != null) {
      return fromGraph;
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
    if (ingressTopics.size() == 1) {
      return buildLinearTree(ingressTopics.iterator().next(), egress.getTopic(), operators);
    }
    return buildJoinTree(new ArrayList<>(ingressTopics), egress.getTopic(), operators);
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildFromGraph(
      final ProcessingPolicyGraph graph,
      final String sinkTopic) {
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
        buildUpstream(writers.get(0), graph, reverse, sinkTopic, visiting);
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
      final List<String> operators) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode body =
        buildOperatorChain(ingressTopic, sinkTopic, operators);
    final AppProcessingPolicy.RelationalAlgebraExpressionNode sink =
        RelationalAlgebraTreeSupport.sinkNode(sinkTopic);
    sink.getChildren().add(body);
    RelationalAlgebraTreeSupport.annotateOutputFields(sink, sinkTopic);
    return sink;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildOperatorChain(
      final String ingressTopic,
      final String sinkTopic,
      final List<String> operators) {
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
      RelationalAlgebraTreeSupport.annotateOutputFields(unary, sinkTopic);
      current = unary;
    }
    return current;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildJoinTree(
      final List<String> ingressTopics,
      final String sinkTopic,
      final List<String> operators) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode join =
        RelationalAlgebraTreeSupport.operatorNode("join", "⋈", "stream–table join");
    for (final String ingress : ingressTopics) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode branch =
          ingress.equals(sinkTopic)
              ? RelationalAlgebraTreeSupport.scanNode(ingress)
              : buildOperatorChain(ingress, sinkTopic, operators);
      join.getChildren().add(branch);
    }
    RelationalAlgebraTreeSupport.annotateOutputFields(join, sinkTopic);
    final AppProcessingPolicy.RelationalAlgebraExpressionNode sink =
        RelationalAlgebraTreeSupport.sinkNode(sinkTopic);
    sink.getChildren().add(join);
    RelationalAlgebraTreeSupport.annotateOutputFields(sink, sinkTopic);
    return sink;
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
      final Set<String> visiting) {
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
      case "internalTopic" -> result = buildThroughInternal(nodeId, graph, reverse, sinkTopic, visiting);
      case "stream" -> result = buildThroughStream(nodeId, graph, reverse, sinkTopic, visiting);
      case "operator" -> result = buildOperator(nodeId, node, graph, reverse, sinkTopic, visiting);
      default -> result = buildThroughStream(nodeId, graph, reverse, sinkTopic, visiting);
    }
    visiting.remove(nodeId);
    return result;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildThroughStream(
      final String nodeId,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting) {
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode up =
          buildUpstream(pred, graph, reverse, sinkTopic, visiting);
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
      final Set<String> visiting) {
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode up =
          buildUpstream(pred, graph, reverse, sinkTopic, visiting);
      if (up != null) {
        final AppProcessingPolicy.RelationalAlgebraExpressionNode repartition =
            RelationalAlgebraTreeSupport.operatorNode("repartition", "ρ", "repartition");
        repartition.getChildren().add(up);
        RelationalAlgebraTreeSupport.annotateOutputFields(repartition, sinkTopic);
        return repartition;
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
      final Set<String> visiting) {
    final String op = RelationalAlgebraTreeSupport.normalizeOp(node.getLabel());
    if (RelationalAlgebraTreeSupport.isJoinOp(op)) {
      return buildJoin(nodeId, op, graph, reverse, sinkTopic, visiting);
    }
    if (RelationalAlgebraTreeSupport.isPassthroughOp(op)) {
      for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
        final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
            buildUpstream(pred, graph, reverse, sinkTopic, visiting);
        if (child != null) {
          return child;
        }
      }
      return null;
    }
    if ("merge".equals(op)) {
      return buildMerge(nodeId, graph, reverse, sinkTopic, visiting);
    }

    final AppProcessingPolicy.RelationalAlgebraExpressionNode unary =
        RelationalAlgebraTreeSupport.operatorNode(
            op,
            RelationalAlgebraTreeSupport.algebraSymbol(op),
            RelationalAlgebraTreeSupport.operatorDescription(op, sinkTopic));
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
          buildUpstream(pred, graph, reverse, sinkTopic, visiting);
      if (child != null) {
        unary.getChildren().add(child);
        break;
      }
    }
    RelationalAlgebraTreeSupport.annotateOutputFields(unary, sinkTopic);
    return unary;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildJoin(
      final String nodeId,
      final String op,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting) {
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
          buildUpstream(pred, graph, reverse, sinkTopic, new LinkedHashSet<>(visiting));
      if (child != null) {
        join.getChildren().add(child);
      }
    }
    for (final String pred : rightBranch) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
          buildUpstream(pred, graph, reverse, sinkTopic, new LinkedHashSet<>(visiting));
      if (child != null) {
        join.getChildren().add(child);
      }
    }
    if (join.getChildren().isEmpty()) {
      return null;
    }
    RelationalAlgebraTreeSupport.annotateOutputFields(join, sinkTopic);
    return join;
  }

  private static AppProcessingPolicy.RelationalAlgebraExpressionNode buildMerge(
      final String nodeId,
      final ProcessingPolicyGraph graph,
      final Map<String, List<String>> reverse,
      final String sinkTopic,
      final Set<String> visiting) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode merge =
        RelationalAlgebraTreeSupport.operatorNode("merge", "∪", "union");
    for (final String pred : reverse.getOrDefault(nodeId, List.of())) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode child =
          buildUpstream(pred, graph, reverse, sinkTopic, new LinkedHashSet<>(visiting));
      if (child != null) {
        merge.getChildren().add(child);
      }
    }
    if (merge.getChildren().isEmpty()) {
      return null;
    }
    RelationalAlgebraTreeSupport.annotateOutputFields(merge, sinkTopic);
    return merge;
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
}
