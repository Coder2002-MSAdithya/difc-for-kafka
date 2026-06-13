package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts stream–table processing steps from a sanitized graph path, including repartition hops,
 * changelog topics, KTable materialization, and state-store backing.
 */
public final class StreamTablePathExtractor {

  public enum StepKind {
    OPERATOR,
    REPARTITION,
    CHANGELOG,
    TABLE_FROM_CHANGELOG,
    TABLE_STATE,
    STATE_STORE,
    TO_STREAM
  }

  public static final class Step {
    private StepKind kind;
    private String name;
    private String topic;
    private String storeName;
    private String edgeLabel;

    public StepKind getKind() {
      return kind;
    }

    public void setKind(final StepKind kind) {
      this.kind = kind;
    }

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(final String topic) {
      this.topic = topic;
    }

    public String getStoreName() {
      return storeName;
    }

    public void setStoreName(final String storeName) {
      this.storeName = storeName;
    }

    public String getEdgeLabel() {
      return edgeLabel;
    }

    public void setEdgeLabel(final String edgeLabel) {
      this.edgeLabel = edgeLabel;
    }
  }

  private StreamTablePathExtractor() {
  }

  public static List<Step> stepsOnSanitizedPath(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic) {
    final List<String> nodePath =
        ProcessingPolicyGraphHelper.findSanitizedNodePathForExtraction(graph, sourceTopic, sinkTopic);
    if (nodePath.isEmpty()) {
      return List.of();
    }
    return stepsFromNodePath(graph, nodePath);
  }

  static List<Step> stepsFromNodePath(
      final ProcessingPolicyGraph graph,
      final List<String> nodePath) {
    final List<Step> steps = new ArrayList<>();
    for (int i = 1; i < nodePath.size(); i++) {
      final String fromId = nodePath.get(i - 1);
      final String toId = nodePath.get(i);
      final ProcessingPolicyGraph.GraphEdge edge =
          ProcessingPolicyGraphHelper.edgeBetween(graph, fromId, toId);
      final ProcessingPolicyGraph.GraphNode fromNode =
          ProcessingPolicyGraphHelper.nodeById(graph, fromId);
      final ProcessingPolicyGraph.GraphNode toNode =
          ProcessingPolicyGraphHelper.nodeById(graph, toId);
      if (edge != null && toNode != null) {
        final Step edgeStep = stepForEdge(edge, fromNode, toNode);
        if (edgeStep != null) {
          steps.add(edgeStep);
          continue;
        }
      }
      if (toNode != null) {
        final Step nodeStep = stepForNode(toNode);
        if (nodeStep != null) {
          steps.add(nodeStep);
        }
        if ("operator".equals(toNode.getKind())) {
          appendMaterializationSideEffects(graph, toId, steps, nodePath, i);
        }
      }
    }
    return steps;
  }

  private static void appendMaterializationSideEffects(
      final ProcessingPolicyGraph graph,
      final String operatorNodeId,
      final List<Step> steps,
      final List<String> nodePath,
      final int operatorIndex) {
    final String nextOnPath =
        operatorIndex + 1 < nodePath.size() ? nodePath.get(operatorIndex + 1) : null;
    for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
      if (!operatorNodeId.equals(edge.getFrom())) {
        continue;
      }
      if (nextOnPath != null && nextOnPath.equals(edge.getTo())) {
        continue;
      }
      if ("materializes".equals(edge.getLabel())) {
        final ProcessingPolicyGraph.GraphNode storeNode =
            ProcessingPolicyGraphHelper.nodeById(graph, edge.getTo());
        final Step storeStep = stateStoreStep(storeNode);
        if (storeStep != null) {
          steps.add(storeStep);
        }
      } else if ("changelog-write".equals(edge.getLabel())) {
        final ProcessingPolicyGraph.GraphNode changelogNode =
            ProcessingPolicyGraphHelper.nodeById(graph, edge.getTo());
        final Step changelogStep = changelogStep(changelogNode, edge.getLabel());
        if (changelogStep != null) {
          steps.add(changelogStep);
        }
      }
    }
  }

  private static Step stepForEdge(
      final ProcessingPolicyGraph.GraphEdge edge,
      final ProcessingPolicyGraph.GraphNode fromNode,
      final ProcessingPolicyGraph.GraphNode toNode) {
    final String label = edge.getLabel() == null ? "" : edge.getLabel();
    return switch (label) {
      case "repartition-write", "through-write" -> repartitionStep(toNode, label);
      case "repartition-read", "through-read" -> null;
      case "changelog-write", "changelog-read" -> changelogStep(toNode, label);
      case "table-source", "table-changelog" -> tableFromChangelogStep(fromNode, toNode, label);
      case "materializes" -> stateStoreStep(toNode);
      default -> null;
    };
  }

  private static Step stepForNode(final ProcessingPolicyGraph.GraphNode node) {
    if ("operator".equals(node.getKind())) {
      final Step step = new Step();
      step.setKind(StepKind.OPERATOR);
      step.setName(node.getLabel());
      return step;
    }
    if ("stream".equals(node.getKind()) && node.getLabel() != null && node.getLabel().contains("KTable")) {
      final Step step = new Step();
      step.setKind(StepKind.TABLE_STATE);
      step.setName(node.getLabel());
      return step;
    }
    if ("stateStore".equals(node.getKind())) {
      return stateStoreStep(node);
    }
    if ("internalTopic".equals(node.getKind())) {
      final String kind = node.getInternalTopicKind();
      if ("repartition".equals(kind)) {
        return repartitionStep(node, "internal");
      }
      if ("changelog".equals(kind)) {
        return changelogStep(node, "internal");
      }
    }
    return null;
  }

  private static Step repartitionStep(final ProcessingPolicyGraph.GraphNode node, final String edgeLabel) {
    final Step step = new Step();
    step.setKind(StepKind.REPARTITION);
    step.setTopic(topicName(node));
    step.setEdgeLabel(edgeLabel);
    step.setName("repartition");
    return step;
  }

  private static Step changelogStep(final ProcessingPolicyGraph.GraphNode node, final String edgeLabel) {
    final Step step = new Step();
    step.setKind(StepKind.CHANGELOG);
    step.setTopic(topicName(node));
    step.setEdgeLabel(edgeLabel);
    step.setName("changelog");
    return step;
  }

  private static Step tableFromChangelogStep(
      final ProcessingPolicyGraph.GraphNode sourceTopicNode,
      final ProcessingPolicyGraph.GraphNode tableNode,
      final String edgeLabel) {
    final Step step = new Step();
    step.setKind(StepKind.TABLE_FROM_CHANGELOG);
    step.setTopic(sourceTopicNode != null ? topicName(sourceTopicNode) : topicName(tableNode));
    step.setEdgeLabel(edgeLabel);
    step.setName(tableNode != null ? tableNode.getLabel() : "KTable");
    return step;
  }

  private static Step stateStoreStep(final ProcessingPolicyGraph.GraphNode node) {
    final Step step = new Step();
    step.setKind(StepKind.STATE_STORE);
    step.setStoreName(storeName(node));
    step.setName("state-store");
    return step;
  }

  private static String topicName(final ProcessingPolicyGraph.GraphNode node) {
    if (node.getTopic() != null && !node.getTopic().isEmpty()) {
      return node.getTopic();
    }
    if (node.getLabel() != null && node.getLabel().contains(":")) {
      final int colon = node.getLabel().indexOf(':');
      if (colon >= 0 && colon + 1 < node.getLabel().length()) {
        return node.getLabel().substring(colon + 1);
      }
    }
    return node.getLabel();
  }

  private static String storeName(final ProcessingPolicyGraph.GraphNode node) {
    if (node.getStoreName() != null && !node.getStoreName().isEmpty()) {
      return node.getStoreName();
    }
    if (node.getLabel() != null && node.getLabel().startsWith("state-store:")) {
      return node.getLabel().substring("state-store:".length());
    }
    return node.getLabel();
  }
}
