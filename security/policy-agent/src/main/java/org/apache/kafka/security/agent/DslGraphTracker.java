package org.apache.kafka.security.agent;

import org.objectweb.asm.*;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public final class DslGraphTracker {
    private static final Object LOCK = new Object();
    private static final IdentityHashMap<Object, String> OBJECT_NODE = new IdentityHashMap<>();
    private static final Map<String, String> TOPIC_NODES = new LinkedHashMap<>();
    private static final Map<String, String> STORE_NODES = new LinkedHashMap<>();
    private static final Map<String, String> INTERNAL_TOPIC_NODES = new LinkedHashMap<>();
    private static final LinkedHashMap<String, Node> NODES = new LinkedHashMap<>();
    private static final List<Edge> EDGES = new ArrayList<>();
    private static int nodeSeq = 1;
    private static String pendingRepartitionStreamNode = null;
    private static String pendingRepartitionTopicNode = null;
    private static String pendingMaterializationOpId = null;
    private static final boolean DEBUG_LAMBDAS = true;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(DslGraphTracker::writeDotFile, "dsl-graph-dot-writer"));
    }

    private DslGraphTracker() {}

    public static void recordTableSource(Object returned, Object topics)
    {
        synchronized (LOCK)
        {
            String tableNode = ensureNode(returned, "KTable[aggregated-state]");
            if (topics instanceof Iterable<?> iterable)
            {
                for (Object topic : iterable)
                {
                    String topicNode = ensureTopicNode(normalize(topic));
                    EDGES.add(new Edge(topicNode, tableNode, "table-changelog"));
                }
            }
            else if (topics instanceof String[] array)
            {
                for (String topic : array)
                {
                    String topicNode = ensureTopicNode(topic);
                    EDGES.add(new Edge(topicNode, tableNode, "table-changelog"));
                }
            }
            else
            {
                String topicNode = ensureTopicNode(normalize(topics));
                EDGES.add(new Edge(topicNode, tableNode, "table-changelog"));
            }
            DslProcessingPolicyTracker.recordSource(returned, topics);
            DslProcessingPolicyTracker.registerComponent("kafka-table");
            writeDotFile();
        }
    }

    public static void recordSource(Object returned, Object topics)
    {
        synchronized (LOCK)
        {
            String streamNode = ensureNode(returned, "sourceStream");

            if (topics instanceof Iterable<?> iterable)
            {
                for (Object topic : iterable)
                {
                    String topicNode =
                            ensureTopicNode(normalize(topic));

                    EDGES.add(new Edge(topicNode, streamNode, "source"));
                }
            }
            else if (topics instanceof String[])
            {
                for (String topic : (String[]) topics)
                {
                    String topicNode =
                            ensureTopicNode(topic);

                    EDGES.add(
                            new Edge(
                                    topicNode,
                                    streamNode,
                                    "source"));
                }
            }
            else
            {
                String topicNode =
                        ensureTopicNode(
                                normalize(topics));

                EDGES.add(
                        new Edge(
                                topicNode,
                                streamNode,
                                "source"));
            }

            DslProcessingPolicyTracker.recordSource(returned, topics);
            writeDotFile();
        }
    }

    public static void recordUnary(String operator, Object upstream, Object returned, Object arg, Object function, boolean source, boolean sink) {
        synchronized (LOCK) {
            String inNode = source ? null : ensureNode(upstream, "stream");
            if (inNode != null && pendingRepartitionTopicNode != null) {
                EDGES.add(new Edge(pendingRepartitionTopicNode, inNode, "repartition-read"));
                pendingRepartitionTopicNode = null;
            }
            String outNode = sink ? ensureNode(upstream, "stream") : ensureNode(returned, operator + "Out");
            String label = formatCleanOperatorLabel(operator, arg, function);

            String opId =
                    createOperatorNode(
                            operator,
                            label,
                            "box");
            if (inNode != null) EDGES.add(new Edge(inNode, opId, "input"));
            if (!sink)
            {
                EDGES.add(
                        new Edge(
                                opId,
                                outNode,
                                "output"));
            }
            else
            {
                String sinkTopic =
                        ensureTopicNode(
                                normalize(arg));

                EDGES.add(
                        new Edge(
                                opId,
                                sinkTopic,
                                "writes"));
            }
            if ("selectKey".equals(operator) || "groupBy".equals(operator) || "groupByKey".equals(operator)) {
                pendingRepartitionStreamNode = outNode;
            }
            if ("aggregate".equals(operator) || "reduce".equals(operator) || "count".equals(operator)) {
                pendingMaterializationOpId = opId;
            }
            if ("windowedBy".equals(operator)) {
                DslProcessingPolicyTracker.recordUnary(operator, upstream, returned, source, arg);
            } else {
                DslProcessingPolicyTracker.recordUnary(operator, upstream, returned, source, function);
            }
            writeDotFile();
        }
    }

    private static String ensureNode(Object ref, String fallback)
    {
        if (ref == null)
        {
            String id = "anon_" + (nodeSeq++);

            NODES.put(
                    id,
                    new Node(
                            id,
                            "stream",
                            fallback,
                            true,
                            "ellipse",
                            "#FDEBD0"));

            return id;
        }

        String existing = OBJECT_NODE.get(ref);

        if (existing != null)
        {
            return existing;
        }

        String id = "stream_" + (nodeSeq++);

        String simple =
                ref.getClass().getSimpleName();

        String label =
                switch (simple)
                {
                    case "KStreamImpl" -> "KStream";
                    case "KTableImpl" -> "KTable";
                    case "KGroupedStreamImpl" -> "KGroupedStream";
                    case "TimeWindowedKStreamImpl" -> "TimeWindowedKStream";
                    case "SessionWindowedKStreamImpl" -> "SessionWindowedKStream";
                    default -> simple;
                };

        NODES.put(
                id,
                new Node(
                        id,
                        "stream",
                        label,
                        false,
                        "ellipse",
                        "#FCF3CF"));

        OBJECT_NODE.put(ref, id);

        return id;
    }

    private static String ensureTopicNode(String topic)
    {
        String normalized =
                topic.replaceAll("[^A-Za-z0-9_\\-]", "_");

        String existing = TOPIC_NODES.get(normalized);

        if (existing != null)
        {
            return existing;
        }

        String id = "topic_" + normalized;

        NODES.put(
                id,
                new Node(
                        id,
                        "topic",
                        "topic:" + topic,
                        false,
                        "cylinder",
                        "#D6EAF8"));

        TOPIC_NODES.put(normalized, id);

        return id;
    }

    private static String ensureStateStoreNode(String store)
    {
        String normalized =
                store.replaceAll("[^A-Za-z0-9_\\-]", "_");

        String existing = STORE_NODES.get(normalized);

        if (existing != null)
        {
            return existing;
        }

        String id = "store_" + normalized;

        NODES.put(
                id,
                new Node(
                        id,
                        "stateStore",
                        "state-store:" + store,
                        false,
                        "folder",
                        "#F9E79F"));

        STORE_NODES.put(normalized, id);

        return id;
    }

    private static String createOperatorNode(
            String operator,
            String semantics,
            String shape)
    {
        String id = "op_" + (nodeSeq++);

        NODES.put(
                id,
                new Node(
                        id,
                        "operator",
                        semantics,
                        false,
                        shape,
                        "#E8F0FE"));

        return id;
    }

    public static void recordJoin(String operator, Object left, Object right, Object returned, Object function) {
        synchronized (LOCK) {
            String leftNode = ensureNode(left, "stream");
            String rightNode = ensureNode(right, "stream");
            String outNode = ensureNode(returned, operator + "Out");

            String label = formatCleanOperatorLabel(operator, null, function);

            String opId =
                    createOperatorNode(
                            operator,
                            label,
                            "hexagon");

            EDGES.add(new Edge(leftNode, opId, "left"));
            EDGES.add(new Edge(rightNode, opId, "right"));
            EDGES.add(new Edge(opId, outNode, "output"));
            DslProcessingPolicyTracker.recordJoin(operator, left, right, returned, function);
            writeDotFile();
        }
    }

    public static void recordBranch(Object upstream, Object[] branches, Object predicates)
    {
        synchronized (LOCK)
        {
            String inNode = ensureNode(upstream, "stream");
            String label = formatCleanOperatorLabel("split", null, predicates);
            String opId = createOperatorNode("split", label, "diamond");
            EDGES.add(new Edge(inNode, opId, "input"));

            if (branches != null)
            {
                for (int i = 0; i < branches.length; i++)
                {
                    String out = ensureNode(branches[i], "branchOut" + i);
                    EDGES.add(new Edge(opId, out, "branch[" + i + "]"));
                }
            }
            DslProcessingPolicyTracker.recordBranch(upstream, branches);
            writeDotFile();
        }
    }

    public static void recordThrough(Object input, Object output, Object topic)
    {
        synchronized (LOCK)
        {
            String in = ensureNode(input, "stream");
            String out = ensureNode(output, "stream");
            String topicNode = ensureInternalTopicNode(normalize(topic), "repartition");
            EDGES.add(new Edge(in, topicNode, "repartition-write"));
            EDGES.add(new Edge(topicNode, out, "repartition-read"));
            DslProcessingPolicyTracker.recordThrough(input, output, topic);
            writeDotFile();
        }
    }

    public static void recordInternalTopic(final String topic)
    {
        synchronized (LOCK)
        {
            final String kind = classifyInternalTopic(topic);
            final String topicNode = ensureInternalTopicNode(topic, kind);
            if ("repartition".equals(kind) && pendingRepartitionStreamNode != null)
            {
                EDGES.add(new Edge(pendingRepartitionStreamNode, topicNode, "repartition-write"));
                pendingRepartitionTopicNode = topicNode;
                pendingRepartitionStreamNode = null;
            }
            else if ("changelog".equals(kind) && pendingMaterializationOpId != null)
            {
                EDGES.add(new Edge(pendingMaterializationOpId, topicNode, "changelog-write"));
                pendingMaterializationOpId = null;
            }
            writeDotFile();
        }
    }

    private static String classifyInternalTopic(final String topic)
    {
        if (topic == null)
        {
            return "internal";
        }
        final String lower = topic.toLowerCase();
        if (lower.contains("-repartition") || lower.endsWith("repartition"))
        {
            return "repartition";
        }
        if (lower.contains("-changelog") || lower.contains("changelog") || lower.contains("-store-changelog"))
        {
            return "changelog";
        }
        return "internal";
    }

    private static String ensureInternalTopicNode(final String topic, final String kind)
    {
        final String normalized = topic.replaceAll("[^A-Za-z0-9_\\-]", "_");
        final String key = kind + ":" + normalized;
        final String existing = INTERNAL_TOPIC_NODES.get(key);
        if (existing != null)
        {
            return existing;
        }
        final String id = "internal_" + normalized;
        NODES.put(
                id,
                new Node(
                        id,
                        "internalTopic",
                        kind + ":" + topic,
                        false,
                        "cylinder",
                        "#E8DAEF"));
        INTERNAL_TOPIC_NODES.put(key, id);
        return id;
    }

    public static void recordStateStore(
            Object store)
    {
        synchronized (LOCK)
        {
            final String storeNode =
                    ensureStateStoreNode(
                            normalize(store));
            if (pendingMaterializationOpId != null)
            {
                EDGES.add(new Edge(pendingMaterializationOpId, storeNode, "materializes"));
                pendingMaterializationOpId = null;
            }
            writeDotFile();
        }
    }

    public static void recordDifcOp(
            final String operator,
            final Object upstream,
            final Object tags,
            final Object returned)
    {
        synchronized (LOCK)
        {
            final String inNode = ensureNode(upstream, "stream");
            final String outNode = ensureNode(returned != null ? returned : upstream, "stream");
            final String label = formatCleanOperatorLabel(operator, tags, null);
            final String opId = createOperatorNode(operator, label, "box");
            EDGES.add(new Edge(inNode, opId, "input"));
            EDGES.add(new Edge(opId, outNode, "output"));
            writeDotFile();
        }
    }

    public static Set<String> deriveIngressTopicsForEgressTopic(final String egressTopic)
    {
        synchronized (LOCK)
        {
            final String egressNodeId = TOPIC_NODES.get(egressTopic.replaceAll("[^A-Za-z0-9_\\-]", "_"));
            if (egressNodeId == null)
            {
                return Set.of();
            }
            final Map<String, List<String>> reverse = reverseAdjacency();
            final Set<String> ingressTopics = new LinkedHashSet<>();
            final Set<String> visited = new HashSet<>();
            final Queue<String> queue = new ArrayDeque<>();
            queue.add(egressNodeId);
            while (!queue.isEmpty())
            {
                final String nodeId = queue.poll();
                if (!visited.add(nodeId))
                {
                    continue;
                }
                final Node node = NODES.get(nodeId);
                if (node != null && "topic".equals(node.kind) && node.label.startsWith("topic:")
                        && !("topic:" + egressTopic).equals(node.label))
                {
                    ingressTopics.add(node.label.substring(6));
                }
                for (final String upstream : reverse.getOrDefault(nodeId, List.of()))
                {
                    if (!visited.contains(upstream))
                    {
                        queue.add(upstream);
                    }
                }
            }
            return ingressTopics;
        }
    }

    public static String exportAggregationAnalysisJson()
    {
        synchronized (LOCK)
        {
            final Map<String, Integer> operatorCounts = new LinkedHashMap<>();
            int joinCount = 0;
            int branchCount = 0;
            int mergeCount = 0;
            for (Node node : NODES.values())
            {
                if (!"operator".equals(node.kind) && !"difc".equals(node.kind))
                {
                    continue;
                }
                final String op = normalizeOperatorLabel(node.label);
                operatorCounts.merge(op, 1, Integer::sum);
                if (op.contains("join"))
                {
                    joinCount++;
                }
                else if ("branch".equals(op) || "split".equals(op))
                {
                    branchCount++;
                }
                else if ("merge".equals(op))
                {
                    mergeCount++;
                }
            }
            int total = 0;
            for (Map.Entry<String, Integer> entry : operatorCounts.entrySet())
            {
                if (isAggregationOperator(entry.getKey()))
                {
                    total += entry.getValue();
                }
            }
            final StringBuilder countsJson = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Integer> entry : operatorCounts.entrySet())
            {
                if (!first)
                {
                    countsJson.append(',');
                }
                countsJson.append('"').append(escapeJson(entry.getKey())).append("\":")
                        .append(entry.getValue());
                first = false;
            }
            countsJson.append('}');
            return "{"
                    + "\"operatorCounts\":" + countsJson + ","
                    + "\"joinCount\":" + joinCount + ","
                    + "\"branchCount\":" + branchCount + ","
                    + "\"mergeCount\":" + mergeCount + ","
                    + "\"totalAggregationOperators\":" + total
                    + "}";
        }
    }

    private static boolean isAggregationOperator(final String op)
    {
        return switch (op)
        {
            case "aggregate", "reduce", "count", "join", "merge", "groupBy", "groupByKey",
                 "windowedBy", "branch", "split" -> true;
            default -> op.contains("join");
        };
    }

    private static String normalizeOperatorLabel(final String label)
    {
        if (label == null || label.isEmpty())
        {
            return "operator";
        }
        final int paren = label.indexOf('(');
        return (paren > 0 ? label.substring(0, paren) : label).trim();
    }

    private static Map<String, List<String>> reverseAdjacency()
    {
        final Map<String, List<String>> reverse = new LinkedHashMap<>();
        for (Edge edge : EDGES)
        {
            reverse.computeIfAbsent(edge.to, k -> new ArrayList<>()).add(edge.from);
        }
        return reverse;
    }

    private static String formatCleanOperatorLabel(
            final String operator,
            final Object arg,
            final Object function)
    {
        if (operator == null || operator.isEmpty())
        {
            return "?";
        }
        switch (operator)
        {
            case "to":
                return arg != null ? "to\n" + normalize(arg) : "to";
            case "through":
                return arg != null ? "through\n" + shortenForDisplay(normalize(arg)) : "through";
            case "addTags":
            case "declassifyTags":
                return arg != null ? operator + "\n" + normalize(arg) : operator;
            case "branch":
                return "split";
            default:
                return operator;
        }
    }

    private static String shortenForDisplay(final String value)
    {
        if (value == null || value.isEmpty())
        {
            return "";
        }
        if (value.length() <= 48)
        {
            return value;
        }
        return value.substring(0, 45) + "...";
    }

    private static String displayLabel(final Node node)
    {
        if (node == null || node.label() == null)
        {
            return "";
        }
        if ("topic".equals(node.kind()) && node.label().startsWith("topic:"))
        {
            return node.label().substring(6);
        }
        if ("internalTopic".equals(node.kind()))
        {
            final int idx = node.label().indexOf(':');
            if (idx >= 0 && idx + 1 < node.label().length())
            {
                return shortenForDisplay(node.label().substring(idx + 1));
            }
        }
        if ("stateStore".equals(node.kind()))
        {
            return "state store";
        }
        return node.label();
    }

    private static String formatSemantics(String operator, Object arg, Object function) {
        List<String> details = new ArrayList<>();
        if (arg != null) details.add(normalize(arg));
        if (function != null) details.add(renderLambda(operator, function));
        return details.isEmpty() ? "()" : "(" + String.join(", ", details) + ")";
    }

    private static String renderLambda(String operator, Object fn)
    {
        if (fn == null)
        {
            return "null";
        }

        SerializedLambda sl = toSerializedLambda(fn);

        if (sl != null)
        {
            debug("serialized-lambda hit", operator, fn.getClass().getName(), sl.getImplClass() + "::" + sl.getImplMethodName());

            String rendered =
                    renderLambda(
                            operator,
                            sl.getImplClass(),
                            sl.getImplMethodName(),
                            sl.getImplMethodSignature());

            if (rendered != null)
            {
                return rendered;
            }
        }

        LambdaRegistry.LambdaInfo info =
                LambdaRegistry.lookup(
                        fn.getClass());

        if (info != null)
        {
            debug(
                    "registry hit",
                    operator,
                    fn.getClass().getName(),
                    info.implClass
                            + "::"
                            + info.implMethod);

            String rendered =
                    renderLambda(
                            operator,
                            info.implClass.replace('.', '/'),
                            info.implMethod,
                            info.implDesc);

            if (rendered != null)
            {
                return rendered;
            }
        }

        String recovered =
                resolveLambdaFromCapturingClass(fn);

        if (recovered != null)
        {
            debug(
                    "capturing-class hit",
                    operator,
                    fn.getClass().getName(),
                    recovered);

            return recovered;
        }

        debug(
                "fallback fn class",
                operator,
                fn.getClass().getName(),
                "no serialized/registry/capturing match");

        return "fn="
                + fn.getClass().getName();
    }

    private static String renderLambda(String operator, String implClass, String implMethod, String methodDesc) {
        int arity = countParams(methodDesc);
        String[] args = lambdaArgNames(operator, arity);
        String reversed = tryReverseEngineerLambdaExpression(operator, implClass, implMethod, args);
        if (reversed != null)
        {
            debug("bytecode expression", operator, implClass + "::" + implMethod, reversed);
            return reversed;
        }

        String rhs = implClass.replace('/', '.') + "::" + implMethod;
        debug("method reference fallback", operator, implClass + "::" + implMethod, rhs);
        return "(" + String.join(", ", args) + ") -> " + rhs;
    }

    private static String[] lambdaArgNames(String operator, int arity) {
        if (arity <= 0) return new String[0];
        String[] args = new String[arity];
        String[] preferred;
        if ("filter".equals(operator) || "map".equals(operator) || "selectKey".equals(operator) || "groupBy".equals(operator)) {
            preferred = new String[]{"k", "v"};
        } else if ("mapValues".equals(operator) || "flatMapValues".equals(operator)) {
            preferred = new String[]{"v"};
        } else if ("reduce".equals(operator)) {
            preferred = new String[]{"v1", "v2"};
        } else if ("aggregate".equals(operator)) {
            preferred = new String[]{"k", "v", "agg"};
        } else {
            preferred = new String[]{"arg0", "arg1", "arg2", "arg3"};
        }

        for (int i = 0; i < arity; i++) {
            args[i] = i < preferred.length ? preferred[i] : "arg" + i;
        }
        return args;
    }

    private static int countParams(String desc) {
        int start = desc.indexOf('(') + 1;
        int end = desc.indexOf(')');
        int i = start, c = 0;
        while (i < end) {
            char ch = desc.charAt(i);
            if (ch == 'L') { while (desc.charAt(i) != ';') i++; c++; }
            else if (ch == '[') { while (desc.charAt(i) == '[') i++; if (desc.charAt(i) == 'L') while (desc.charAt(i) != ';') i++; c++; }
            else c++;
            i++;
        }
        return c;
    }

    private static String tryReverseEngineerLambdaExpression(
            String operator,
            String implClass,
            String implMethod,
            String[] args)
    {
        try
        {
            Class<?> cls = Class.forName(implClass.replace('/', '.'));
            String resource = "/" + implClass + ".class";

            try (InputStream in = cls.getResourceAsStream(resource))
            {
                if (in == null)
                {
                    return null;
                }

                ClassNode cn = new ClassNode();
                new ClassReader(in).accept(cn, 0);

                for (MethodNode mn : cn.methods)
                {
                    if (!mn.name.equals(implMethod))
                    {
                        continue;
                    }

                    String expr = trySimpleExpression(operator, mn, args);
                    if (expr != null)
                    {
                        return expr;
                    }
                }
            }
        }
        catch (Throwable ignored)
        {
        }

        return null;
    }

    private static String trySimpleExpression(String operator, MethodNode mn, String[] args)
    {
        String symbolic = trySymbolicExpression(mn, args);
        if (symbolic != null)
        {
            return symbolic;
        }

        if ("reduce".equals(operator) && args.length == 2)
        {
            if (containsOpcode(mn, Opcodes.IF_ICMPGT) || containsOpcode(mn, Opcodes.IF_ICMPGE))
            {
                return "(" + args[0] + ", " + args[1] + ") -> max(" + args[0] + ", " + args[1] + ")";
            }
        }

        return null;
    }

    private static String trySymbolicExpression(MethodNode mn, String[] args)
    {
        java.util.Deque<String> stack = new java.util.ArrayDeque<>();
        java.util.Map<Integer, String> locals = new java.util.HashMap<>();

        for (int i = 0; i < args.length; i++)
        {
            locals.put(i, args[i]);
        }

        for (AbstractInsnNode insn = mn.instructions.getFirst(); insn != null; insn = insn.getNext())
        {
            int op = insn.getOpcode();
            if (op < 0) continue;

            if (insn instanceof org.objectweb.asm.tree.VarInsnNode var)
            {
                if (op >= Opcodes.ILOAD && op <= Opcodes.ALOAD)
                {
                    stack.push(locals.getOrDefault(var.var, "local" + var.var));
                    continue;
                }
                if (op >= Opcodes.ISTORE && op <= Opcodes.ASTORE)
                {
                    if (!stack.isEmpty()) locals.put(var.var, stack.pop());
                    continue;
                }
            }

            if (insn instanceof org.objectweb.asm.tree.FieldInsnNode fi)
            {
                if (op == Opcodes.GETFIELD
                        || op == Opcodes.GETSTATIC)
                {
                    String owner =
                            fi.owner.replace('/', '.');

                    stack.push(
                            owner
                                    + "."
                                    + fi.name);

                    continue;
                }
            }

            if (insn instanceof LdcInsnNode ldc)
            {
                stack.push(String.valueOf(ldc.cst));
                continue;
            }

            if (op >= Opcodes.ICONST_M1 && op <= Opcodes.ICONST_5)
            {
                stack.push(String.valueOf(op - Opcodes.ICONST_0));
                continue;
            }

            if (insn instanceof org.objectweb.asm.tree.IntInsnNode intInsn && (op == Opcodes.BIPUSH || op == Opcodes.SIPUSH))
            {
                stack.push(String.valueOf(intInsn.operand));
                continue;
            }

            if (insn instanceof org.objectweb.asm.tree.MethodInsnNode mi)
            {
                int argCount =
                        countParams(mi.desc);

                List<String> callArgs =
                        new ArrayList<>();

                for (int i = 0; i < argCount; i++)
                {
                    if (!stack.isEmpty())
                    {
                        callArgs.add(0, stack.pop());
                    }
                }

                String owner =
                        mi.owner.replace('/', '.');

                String methodCall =
                        owner
                                + "."
                                + mi.name
                                + "("
                                + String.join(", ", callArgs)
                                + ")";

                if (mi.getOpcode() != Opcodes.INVOKESTATIC
                        && !stack.isEmpty())
                {
                    String receiver = stack.pop();

                    methodCall =
                            receiver
                                    + "."
                                    + mi.name
                                    + "("
                                    + String.join(", ", callArgs)
                                    + ")";
                }

                stack.push(methodCall);

                continue;
            }

            if ((op == Opcodes.IADD || op == Opcodes.ISUB || op == Opcodes.IMUL || op == Opcodes.IDIV) && stack.size() >= 2)
            {
                String r = stack.pop();
                String l = stack.pop();
                String sym = switch (op)
                {
                    case Opcodes.IADD -> "+";
                    case Opcodes.ISUB -> "-";
                    case Opcodes.IMUL -> "*";
                    default -> "/";
                };
                stack.push("(" + l + " " + sym + " " + r + ")");
                continue;
            }

            if ((op == Opcodes.IF_ICMPGT || op == Opcodes.IF_ICMPGE || op == Opcodes.IF_ICMPLT || op == Opcodes.IF_ICMPLE || op == Opcodes.IF_ICMPEQ || op == Opcodes.IF_ICMPNE)
                    && stack.size() >= 2)
            {
                String r = stack.pop();
                String l = stack.pop();
                String cmp = switch (op)
                {
                    case Opcodes.IF_ICMPGT -> ">";
                    case Opcodes.IF_ICMPGE -> ">=";
                    case Opcodes.IF_ICMPLT -> "<";
                    case Opcodes.IF_ICMPLE -> "<=";
                    case Opcodes.IF_ICMPEQ -> "==";
                    default -> "!=";
                };
                return "(" + String.join(", ", args) + ") -> " + l + " " + cmp + " " + r;
            }

            if ((op == Opcodes.IRETURN || op == Opcodes.ARETURN) && !stack.isEmpty())
            {
                return "(" + String.join(", ", args) + ") -> " + stack.peek();
            }
        }

        return null;
    }

    private static boolean containsOpcode(MethodNode mn, int opcode)
    {
        for (AbstractInsnNode insn = mn.instructions.getFirst(); insn != null; insn = insn.getNext())
        {
            if (insn.getOpcode() == opcode)
            {
                return true;
            }
        }
        return false;
    }

    private static Integer findComparedIntegerConstant(MethodNode mn)
    {
        for (AbstractInsnNode insn = mn.instructions.getFirst(); insn != null; insn = insn.getNext())
        {
            if (insn instanceof LdcInsnNode ldc && ldc.cst instanceof Integer)
            {
                return (Integer) ldc.cst;
            }

            int op = insn.getOpcode();
            if (op >= Opcodes.ICONST_M1 && op <= Opcodes.ICONST_5)
            {
                return op - Opcodes.ICONST_0;
            }
        }
        return null;
    }

    private static void debug(String stage, String operator, String subject, String detail)
    {
        if (!DEBUG_LAMBDAS)
        {
            return;
        }

        System.out.println("[POLICY][DSLDBG] stage=" + stage
                + " operator=" + operator
                + " subject=" + subject
                + " detail=" + detail);
    }

    private static String resolveLambdaFromCapturingClass(Object fn)
    {
        try
        {
            String lambdaName = fn.getClass().getName();

            int idx = lambdaName.indexOf("$$Lambda");

            if (idx < 0)
            {
                return null;
            }

            String capturingClass = lambdaName.substring(0, idx);

            Class<?> owner = Class.forName(capturingClass);

            String resource = "/" + capturingClass.replace('.', '/') + ".class";

            try (InputStream in = owner.getResourceAsStream(resource))
            {
                if (in == null)
                {
                    return null;
                }

                ClassReader cr = new ClassReader(in);

                final java.util.List<String> candidates = new java.util.ArrayList<>();

                cr.accept(new ClassVisitor(Opcodes.ASM9)
                        {
                            @Override
                            public MethodVisitor visitMethod(
                                    int access,
                                    String name,
                                    String desc,
                                    String sig,
                                    String[] ex)
                            {
                                return new MethodVisitor(Opcodes.ASM9)
                                {
                                    @Override
                                    public void visitInvokeDynamicInsn(
                                            String indyName,
                                            String indyDesc,
                                            Handle bsm,
                                            Object... bsmArgs)
                                    {
                                        for (Object arg : bsmArgs)
                                        {
                                            if (arg instanceof Handle h)
                                            {
                                                if (h.getName()
                                                        .startsWith(
                                                                "lambda$"))
                                                {
                                                    MethodNode target =
                                                            findMethod(
                                                                    owner,
                                                                    h.getName(),
                                                                    h.getDesc());

                                                    if (target == null)
                                                    {
                                                        continue;
                                                    }

                                                    String[] params =
                                                            lambdaArgNames(
                                                                    "lambda",
                                                                    countParams(
                                                                            target.desc));

                                                    String expr =
                                                            trySymbolicExpression(
                                                                    target,
                                                                    params);

                                                    if (expr != null)
                                                    {
                                                        candidates.add(expr);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                };
                            }
                        },
                        0);

                if (candidates.isEmpty())
                {
                    return null;
                }

                candidates.sort(
                        (a, b) ->
                                Integer.compare(
                                        scoreExpr(b),
                                        scoreExpr(a)));

                return candidates.get(0);
            }
        }
        catch (Throwable t)
        {
            debug(
                    "capturing-class-failed",
                    fn.getClass().getName(),
                    "",
                    t.toString());
        }

        return null;
    }

    private static int scoreExpr(String expr)
    {
        int score = 0;

        if (expr == null)
        {
            return score;
        }

        if (expr.contains(".")) score += 5;

        if (expr.contains("(")) score += 5;

        if (expr.contains("+")) score += 4;

        if (expr.contains("-")) score += 4;

        if (expr.contains("*")) score += 4;

        if (expr.contains("/")) score += 4;

        if (expr.contains(">")) score += 4;

        if (expr.contains("<")) score += 4;

        if (expr.contains("==")) score += 4;

        if (expr.contains("split")) score += 10;

        if (expr.contains("map")) score += 5;

        if (expr.contains("filter")) score += 5;

        if (expr.length() > 20) score += 3;

        if (expr.contains("arg0")) score -= 2;

        if (expr.contains("arg1")) score -= 2;

        return score;
    }

    private static MethodNode findMethod(Class<?> owner, String methodName, String methodDesc)
    {
        try
        {
            String resource = "/" + owner.getName().replace('.', '/') + ".class";

            try (InputStream in =
                         owner.getResourceAsStream(resource))
            {
                if (in == null)
                {
                    return null;
                }

                ClassReader cr =
                        new ClassReader(in);

                ClassNode cn =
                        new ClassNode();

                cr.accept(cn, 0);

                for (MethodNode mn : cn.methods)
                {
                    if (mn.name.equals(methodName)
                            && mn.desc.equals(methodDesc))
                    {
                        return mn;
                    }
                }
            }
        }
        catch (Throwable ignored)
        {

        }

        return null;
    }

    private static SerializedLambda toSerializedLambda(Object lambda) {
        try {
            Method m = lambda.getClass().getDeclaredMethod("writeReplace");
            m.setAccessible(true);
            Object replacement = m.invoke(lambda);
            return replacement instanceof SerializedLambda ? (SerializedLambda) replacement : null;
        } catch (Throwable t) {
            return null;
        }
    }

    private static String normalize(Object o) { return o == null ? "null" : String.valueOf(o).replace("\"", "'"); }

    /**
     * Export the instrumented DSL graph as JSON for grant-time policy verification.
     */
    public static String exportPolicyGraphJson()
    {
        synchronized (LOCK)
        {
            final StringBuilder nodesJson = new StringBuilder("[");
            boolean firstNode = true;
            for (Node node : NODES.values())
            {
                if (!firstNode)
                {
                    nodesJson.append(',');
                }
                nodesJson.append('{')
                        .append("\"id\":\"").append(escapeJson(node.id)).append("\",")
                        .append("\"kind\":\"").append(escapeJson(node.kind)).append("\",")
                        .append("\"label\":\"").append(escapeJson(node.label)).append("\"");
                if ("topic".equals(node.kind) && node.label.startsWith("topic:"))
                {
                    nodesJson.append(",\"topic\":\"").append(escapeJson(node.label.substring(6))).append("\"");
                }
                else if ("internalTopic".equals(node.kind) && node.label.contains(":"))
                {
                    final int colon = node.label.indexOf(':');
                    nodesJson.append(",\"internalTopicKind\":\"")
                            .append(escapeJson(node.label.substring(0, colon)))
                            .append("\",")
                            .append("\"topic\":\"")
                            .append(escapeJson(node.label.substring(colon + 1)))
                            .append("\"");
                }
                else if ("stateStore".equals(node.kind) && node.label.startsWith("state-store:"))
                {
                    nodesJson.append(",\"storeName\":\"")
                            .append(escapeJson(node.label.substring("state-store:".length())))
                            .append("\"");
                }
                else if ("stream".equals(node.kind) && node.label.contains("KTable"))
                {
                    nodesJson.append(",\"tableRole\":\"aggregated-state\"");
                }
                nodesJson.append('}');
                firstNode = false;
            }
            nodesJson.append(']');

            final StringBuilder edgesJson = new StringBuilder("[");
            boolean firstEdge = true;
            for (Edge edge : EDGES)
            {
                if (!firstEdge)
                {
                    edgesJson.append(',');
                }
                edgesJson.append('{')
                        .append("\"from\":\"").append(escapeJson(edge.from)).append("\",")
                        .append("\"to\":\"").append(escapeJson(edge.to)).append("\",")
                        .append("\"label\":\"").append(escapeJson(edge.label)).append("\"")
                        .append('}');
                firstEdge = false;
            }
            edgesJson.append(']');

            return "{\"nodes\":" + nodesJson + ",\"edges\":" + edgesJson + "}";
        }
    }

    private static String escapeJson(final String value)
    {
        if (value == null)
        {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n");
    }

    public static void writeDotFile()
    {
        synchronized (LOCK)
        {
            String path =
                    System.getProperty(
                            "policy.dsl.dot.path",
                            "policy-dsl-topology.dot");

            Path dotPath =
                    Paths.get(path).toAbsolutePath();

            StringBuilder sb = new StringBuilder();

            sb.append("digraph KafkaDsl {\n");

            sb.append("  rankdir=LR;\n\n");

            sb.append("  subgraph cluster_legend {\n");
            sb.append("    label=\"Legend\";\n");

            sb.append(
                    "    keyTopic "
                            + "[label=\"Kafka Topic\", "
                            + "shape=\"cylinder\", "
                            + "style=\"filled\", "
                            + "fillcolor=\"#D6EAF8\"];\n");

            sb.append(
                    "    keyStream "
                            + "[label=\"Kafka Stream\", "
                            + "shape=\"ellipse\", "
                            + "style=\"filled\", "
                            + "fillcolor=\"#FCF3CF\"];\n");

            sb.append(
                    "    keyOperator "
                            + "[label=\"DSL Operator\", "
                            + "shape=\"box\", "
                            + "style=\"filled\", "
                            + "fillcolor=\"#E8F0FE\"];\n");

            sb.append(
                    "    keyBranch "
                            + "[label=\"Branch Operator\", "
                            + "shape=\"diamond\", "
                            + "style=\"filled\", "
                            + "fillcolor=\"#E8F0FE\"];\n");

            sb.append(
                    "    keyJoin "
                            + "[label=\"Join/Merge Operator\", "
                            + "shape=\"hexagon\", "
                            + "style=\"filled\", "
                            + "fillcolor=\"#E8F0FE\"];\n");

            sb.append(
                    "    keyStore "
                            + "[label=\"State Store\", "
                            + "shape=\"folder\", "
                            + "style=\"filled\", "
                            + "fillcolor=\"#F9E79F\"];\n");

            sb.append("  }\n\n");

            for (Node node : NODES.values())
            {
                sb.append("  \"")
                        .append(node.id)
                        .append("\" ");

                sb.append("[label=\"")
                        .append(escape(displayLabel(node)))
                        .append("\"");

                sb.append(", class=\"")
                        .append(node.kind)
                        .append("\"");

                sb.append(", shape=\"")
                        .append(node.shape)
                        .append("\"");

                sb.append(", style=\"filled,rounded\"");

                sb.append(", fillcolor=\"")
                        .append(node.fillColor)
                        .append("\"");

                sb.append("];\n");
            }

            sb.append("\n");

            for (Edge edge : EDGES)
            {
                sb.append("  \"")
                        .append(edge.from)
                        .append("\" -> \"")
                        .append(edge.to)
                        .append("\"");

                sb.append(" [label=\"")
                        .append(escape(edge.label))
                        .append("\"];\n");
            }

            sb.append("\n");

            sb.append("  graph [label=\"Generated at ")
                    .append(Instant.now())
                    .append("\"];\n");

            sb.append("}\n");

            try
            {
                Path parent = dotPath.getParent();

                if (parent != null)
                {
                    Files.createDirectories(parent);
                }

                Files.writeString(
                        dotPath,
                        sb.toString(),
                        StandardCharsets.UTF_8);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static String escape(String s)
    {
        if (s == null)
        {
            return "";
        }
        return s
                .replace("\\", "\\\\")
                .replace("\n", "\\n")
                .replace("\r", "")
                .replace("\"", "\\\"")
                .replace("{", "\\{")
                .replace("}", "\\}")
                .replace("<", "\\<")
                .replace(">", "\\>")
                .replace("|", "\\|");
    }

    private record Node(
            String id,
            String kind,
            String label,
            boolean synthetic,
            String shape,
            String fillColor)
    {}
    private record Edge(String from, String to, String label) {}
}