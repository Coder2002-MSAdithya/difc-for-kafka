package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Shared tree construction, field evaluation, and formatting for relational-algebra expressions. */
public final class RelationalAlgebraTreeSupport {

  private RelationalAlgebraTreeSupport() {
  }

  public static AppProcessingPolicy.RelationalAlgebraExpressionNode scanNode(final String topic) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode node = new AppProcessingPolicy.RelationalAlgebraExpressionNode();
    node.setKind("scan");
    node.setAlgebraSymbol("Scan");
    node.setTopic(topic);
    node.setDescription("Scan(" + topic + ")");
    node.setOutputFields(new ArrayList<>(TopicSchemaResolver.valueFieldsForTopic(topic)));
    return node;
  }

  public static AppProcessingPolicy.RelationalAlgebraExpressionNode sinkNode(final String topic) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode node = new AppProcessingPolicy.RelationalAlgebraExpressionNode();
    node.setKind("sink");
    node.setAlgebraSymbol("Sink");
    node.setTopic(topic);
    node.setDescription("Sink(" + topic + ")");
    return node;
  }

  public static AppProcessingPolicy.RelationalAlgebraExpressionNode operatorNode(
      final String operator,
      final String symbol,
      final String description) {
    final AppProcessingPolicy.RelationalAlgebraExpressionNode node = new AppProcessingPolicy.RelationalAlgebraExpressionNode();
    node.setKind("operator");
    node.setAlgebraSymbol(symbol);
    node.setDescription(description);
    node.setTopic(operator);
    return node;
  }

  public static void annotateOutputFields(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String sinkTopic) {
    node.setOutputFields(new ArrayList<>(evaluateOutputFields(node, sinkTopic)));
  }

  public static Set<String> evaluateOutputFields(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String sinkTopic) {
    if (node == null) {
      return Set.of();
    }
    if (!node.getOutputFields().isEmpty()) {
      return new LinkedHashSet<>(node.getOutputFields());
    }
    return switch (node.getKind()) {
      case "scan" -> TopicSchemaCatalog.copyFields(TopicSchemaResolver.valueFieldsForTopic(node.getTopic()));
      case "sink" -> evaluateChildOutput(node, sinkTopic);
      case "operator" -> evaluateOperatorOutput(node, sinkTopic);
      default -> Set.of();
    };
  }

  private static Set<String> evaluateChildOutput(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String sinkTopic) {
    if (node.getChildren().isEmpty()) {
      return TopicSchemaCatalog.copyFields(TopicSchemaResolver.valueFieldsForTopic(sinkTopic));
    }
    final Set<String> pipelineFields = evaluateOutputFields(node.getChildren().get(0), sinkTopic);
    if (schemaNarrowingEgress(sinkTopic, pipelineFields)) {
      return TopicSchemaCatalog.copyFields(TopicSchemaResolver.valueFieldsForTopic(sinkTopic));
    }
    return pipelineFields;
  }

  private static boolean schemaNarrowingEgress(
      final String sinkTopic,
      final Set<String> pipelineFields) {
    if (!TopicSchemaCatalog.isKnownTopic(sinkTopic) || pipelineFields.isEmpty()) {
      return false;
    }
    final Set<String> sinkSchema = TopicSchemaResolver.valueFieldsForTopic(sinkTopic);
    if (sinkSchema.isEmpty() || sinkSchema.equals(pipelineFields)) {
      return false;
    }
    final Set<String> retainedOrderSensitive = new LinkedHashSet<>(pipelineFields);
    retainedOrderSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
    final Set<String> sinkOrderSensitive = new LinkedHashSet<>(sinkSchema);
    sinkOrderSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
    return !retainedOrderSensitive.isEmpty() && sinkOrderSensitive.isEmpty();
  }

  private static Set<String> evaluateOperatorOutput(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String sinkTopic) {
    final String op = node.getTopic() == null ? "" : node.getTopic();
    if (isJoinOp(op)) {
      final Set<String> merged = new LinkedHashSet<>();
      for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
        merged.addAll(evaluateOutputFields(child, sinkTopic));
      }
      return merged;
    }
    if ("merge".equals(op)) {
      final Set<String> merged = new LinkedHashSet<>();
      for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
        merged.addAll(evaluateOutputFields(child, sinkTopic));
      }
      return merged;
    }
    if (Set.of("mapvalues", "map", "flatmapvalues", "flatmap", "transform", "transformvalues", "process")
        .contains(op)
        || JugPipelineProjections.isPipelineProjectionOperatorNormalized(op)) {
      if (!node.getOutputFields().isEmpty()) {
        return new LinkedHashSet<>(node.getOutputFields());
      }
      if (node.getChildren().isEmpty()) {
        return projectionFallback(sinkTopic);
      }
      final Set<String> childFields = evaluateOutputFields(node.getChildren().get(0), sinkTopic);
      final Set<String> egressProjection = EgressProjectionRegistry.projectionForEgress(sinkTopic);
      if (!egressProjection.isEmpty()
          && !childFields.isEmpty()
          && childFields.size() > egressProjection.size()
          && childFields.containsAll(egressProjection)) {
        return new LinkedHashSet<>(egressProjection);
      }
      return childFields;
    }
    if (Set.of("aggregate", "reduce", "count").contains(op)) {
      return Set.of("_aggregate_value");
    }
    if (node.getChildren().isEmpty()) {
      return Set.of();
    }
    return evaluateOutputFields(node.getChildren().get(0), sinkTopic);
  }

  private static Set<String> projectionFallback(final String sinkTopic) {
    final Set<String> egressProjection = EgressProjectionRegistry.projectionForEgress(sinkTopic);
    if (!egressProjection.isEmpty()) {
      return new LinkedHashSet<>(egressProjection);
    }
    final Set<String> observedEgress = TopicFieldRegistry.fieldsForTopic(sinkTopic);
    if (TopicSchemaResolver.isUsefulFieldObservation(observedEgress)) {
      return new LinkedHashSet<>(observedEgress);
    }
    if (TopicSchemaCatalog.isKnownTopic(sinkTopic)) {
      return TopicSchemaCatalog.copyFields(TopicSchemaResolver.valueFieldsForTopic(sinkTopic));
    }
    return Set.of();
  }

  public static AppProcessingPolicy.RelationalAlgebraExpressionNode findScan(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String topic) {
    if (node == null) {
      return null;
    }
    if ("scan".equals(node.getKind()) && topic.equals(node.getTopic())) {
      return node;
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode found = findScan(child, topic);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  public static boolean containsScan(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String topic) {
    return findScan(node, topic) != null;
  }

  public static String formatTree(final AppProcessingPolicy.RelationalAlgebraExpressionNode root) {
    if (root == null) {
      return "";
    }
    return formatNode(root);
  }

  private static String formatNode(final AppProcessingPolicy.RelationalAlgebraExpressionNode node) {
    if ("scan".equals(node.getKind())) {
      return "Scan(" + node.getTopic() + ")";
    }
    if ("sink".equals(node.getKind())) {
      if (node.getChildren().isEmpty()) {
        return "Sink(" + node.getTopic() + ")";
      }
      return "Sink(" + node.getTopic() + ", " + formatNode(node.getChildren().get(0)) + ")";
    }
    if ("operator".equals(node.getKind())) {
      final String symbol = node.getAlgebraSymbol() == null ? "?" : node.getAlgebraSymbol();
      final String predicate = formatSelectionPredicate(node);
      if (node.getChildren().isEmpty()) {
        return predicate.isEmpty() ? symbol : symbol + "[" + predicate + "]";
      }
      if (node.getChildren().size() == 1) {
        final String child = formatNode(node.getChildren().get(0));
        if (predicate.isEmpty()) {
          return symbol + "(" + child + ")";
        }
        return symbol + "[" + predicate + "](" + child + ")";
      }
      final StringBuilder args = new StringBuilder();
      for (int i = 0; i < node.getChildren().size(); i++) {
        if (i > 0) {
          args.append(", ");
        }
        args.append(formatNode(node.getChildren().get(i)));
      }
      return symbol + "(" + args + ")";
    }
    return node.getDescription() == null ? "?" : node.getDescription();
  }

  private static String formatSelectionPredicate(final AppProcessingPolicy.RelationalAlgebraExpressionNode node) {
    if (node == null) {
      return "";
    }
    final List<String> parts = new ArrayList<>();
    if (node.getSelectionExpression() != null && !node.getSelectionExpression().isEmpty()) {
      parts.add("σ:" + node.getSelectionExpression());
    } else if (node.getSelectionFields() != null && !node.getSelectionFields().isEmpty()) {
      parts.add("σ:" + String.join(" ∧ ", node.getSelectionFields()));
    }
    if (node.getKeyFields() != null && !node.getKeyFields().isEmpty()) {
      parts.add("key:" + String.join(",", node.getKeyFields()));
    }
    if (node.getOutputFields() != null && !node.getOutputFields().isEmpty()) {
      final String op = node.getTopic() == null ? "" : node.getTopic();
      if (Set.of("mapvalues", "map", "flatmapvalues", "flatmap", "transform", "transformvalues", "process",
              "forstockcheck", "forvalidation", "forbilling", "selectkey")
          .contains(op)) {
        parts.add("π:" + String.join(", ", node.getOutputFields()));
      }
    }
    return String.join("; ", parts);
  }

  /** True when the plan has split/merge/join fan-out captured from the processing graph. */
  public static boolean hasBranchingTopology(final AppProcessingPolicy.RelationalAlgebraExpressionNode root) {
    return countBranchingOperators(root) > 0;
  }

  public static boolean mergeTopologyCollapsed(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode root,
      final AppProcessingPolicy policy,
      final AppProcessingPolicy.EgressPath egress) {
    if (root == null || egress == null) {
      return false;
    }
    if (!graphDocumentsSplitMerge(policy) && !egressDocumentsSplitMerge(egress)) {
      return false;
    }
    return maxMergeChildScans(root) < 2;
  }

  private static boolean egressDocumentsSplitMerge(final AppProcessingPolicy.EgressPath egress) {
    if (egress.getOperators() == null) {
      return false;
    }
    boolean hasSplit = false;
    boolean hasMerge = false;
    for (final String raw : egress.getOperators()) {
      final String op = normalizeOp(raw + "()");
      if ("split".equals(op) || "branch".equals(op)) {
        hasSplit = true;
      }
      if ("merge".equals(op)) {
        hasMerge = true;
      }
    }
    return hasSplit && hasMerge;
  }

  private static boolean graphDocumentsSplitMerge(final AppProcessingPolicy policy) {
    if (policy == null || policy.getGraph() == null) {
      return false;
    }
    boolean hasSplit = false;
    boolean hasMerge = false;
    for (final ProcessingPolicyGraph.GraphNode node : policy.getGraph().getNodes()) {
      if (!"operator".equals(node.getKind())) {
        continue;
      }
      final String op = normalizeOp(node.getLabel());
      if ("split".equals(op) || "branch".equals(op)) {
        hasSplit = true;
      }
      if ("merge".equals(op)) {
        hasMerge = true;
      }
    }
    return hasSplit && hasMerge;
  }

  private static int maxMergeChildScans(final AppProcessingPolicy.RelationalAlgebraExpressionNode node) {
    if (node == null) {
      return 0;
    }
    int max = 0;
    if ("operator".equals(node.getKind()) && "merge".equals(node.getTopic())) {
      int scans = 0;
      for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
        scans += countScanNodes(child);
      }
      max = Math.max(max, scans);
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      max = Math.max(max, maxMergeChildScans(child));
    }
    return max;
  }

  private static int countScanNodes(final AppProcessingPolicy.RelationalAlgebraExpressionNode node) {
    if (node == null) {
      return 0;
    }
    int count = "scan".equals(node.getKind()) ? 1 : 0;
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      count += countScanNodes(child);
    }
    return count;
  }

  public static boolean treeContainsOperator(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String normalizedOperator) {
    if (node == null || normalizedOperator == null || normalizedOperator.isEmpty()) {
      return false;
    }
    if ("operator".equals(node.getKind())) {
      final String op = node.getTopic() == null ? "" : node.getTopic();
      if (normalizedOperator.equals(op)) {
        return true;
      }
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      if (treeContainsOperator(child, normalizedOperator)) {
        return true;
      }
    }
    return false;
  }

  private static int countBranchingOperators(final AppProcessingPolicy.RelationalAlgebraExpressionNode node) {
    if (node == null) {
      return 0;
    }
    int count = 0;
    if ("operator".equals(node.getKind())) {
      final String op = node.getTopic() == null ? "" : node.getTopic();
      final String symbol = node.getAlgebraSymbol() == null ? "" : node.getAlgebraSymbol();
      if (node.getChildren().size() > 1
          && (isJoinOp(op) || "merge".equals(op) || "split".equals(op) || "branch".equals(op)
              || "⋈".equals(symbol) || "∪".equals(symbol) || "σ∪".equals(symbol))) {
        count++;
      }
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      count += countBranchingOperators(child);
    }
    return count;
  }

  public static boolean isJoinOp(final String op) {
    return Set.of("join", "leftjoin", "outerjoin").contains(op);
  }

  public static boolean isPassthroughOp(final String op) {
    return Set.of("to", "addtags", "declassifytags", "tostream").contains(op);
  }

  public static String normalizeOp(final String raw) {
    if (raw == null || raw.isEmpty()) {
      return "operator";
    }
    final String head = raw.split("\n", 2)[0];
    final int paren = head.indexOf('(');
    return (paren > 0 ? head.substring(0, paren) : head).trim().toLowerCase();
  }

  public static String algebraSymbol(final String op) {
    return switch (op) {
      case "filter" -> "σ";
      case "mapvalues", "map", "flatmapvalues", "flatmap", "transform", "transformvalues", "process",
          "forstockcheck", "forvalidation", "forbilling" -> "π";
      case "aggregate", "reduce", "count", "groupby", "groupbykey", "windowedby" -> "γ";
      case "join", "leftjoin", "outerjoin" -> "⋈";
      case "merge" -> "∪";
      case "selectkey" -> "π_k";
      case "branch", "split" -> "σ∪";
      case "repartition" -> "ρ";
      default -> op;
    };
  }

  public static String operatorDescription(final String op, final String sinkTopic) {
    return switch (op) {
      case "filter" -> "selection";
      case "mapvalues", "map", "flatmapvalues", "flatmap", "transform", "transformvalues", "process",
          "forstockcheck", "forvalidation", "forbilling" ->
          "projection/map";
      case "aggregate", "reduce", "count" -> "group-by/aggregate";
      case "groupby", "groupbykey", "windowedby" -> "group/window";
      case "selectkey" -> "key reassignment";
      case "branch", "split" -> "branch";
      case "merge" -> "union";
      case "join", "leftjoin", "outerjoin" -> "stream–table join";
      case "repartition" -> "repartition";
      default -> op;
    };
  }

  public static void collectFlags(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final AppProcessingPolicy.ProcessingPathAnalysis path) {
    if (node == null) {
      return;
    }
    if ("operator".equals(node.getKind())) {
      final String op = node.getTopic() == null ? "" : node.getTopic();
      switch (op) {
        case "filter" -> path.setFiltered(true);
        case "aggregate", "reduce", "count", "groupby", "groupbykey", "windowedby" -> path.setAggregated(true);
        case "join", "leftjoin", "outerjoin" -> path.setJoined(true);
        case "repartition" -> path.setRepartitionInvolved(true);
        case "changelog" -> path.setChangelogInvolved(true);
        default -> {
        }
      }
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      collectFlags(child, path);
    }
  }
}
