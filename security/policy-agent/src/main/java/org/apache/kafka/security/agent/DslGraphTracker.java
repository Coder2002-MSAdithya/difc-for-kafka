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
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DslGraphTracker {
    private static final Object LOCK = new Object();
    private static final IdentityHashMap<Object, String> OBJECT_NODE = new IdentityHashMap<>();
    private static final Map<String, String> TOPIC_NODES = new LinkedHashMap<>();
    private static final Map<String, String> STORE_NODES = new LinkedHashMap<>();
    private static final LinkedHashMap<String, Node> NODES = new LinkedHashMap<>();
    private static final List<Edge> EDGES = new ArrayList<>();
    private static int nodeSeq = 1;
    private static final boolean DEBUG_LAMBDAS = true;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(DslGraphTracker::writeDotFile, "dsl-graph-dot-writer"));
    }

    private DslGraphTracker() {}

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

            writeDotFile();
        }
    }

    public static void recordUnary(String operator, Object upstream, Object returned, Object arg, Object function, boolean source, boolean sink) {
        synchronized (LOCK) {
            String inNode = source ? null : ensureNode(upstream, "stream");
            String outNode = sink ? ensureNode(upstream, "stream") : ensureNode(returned, operator + "Out");
            String semantics =
                    operator
                            + formatSemantics(
                            operator,
                            arg,
                            function);

            String opId =
                    createOperatorNode(
                            operator,
                            semantics,
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

            String semantics =
                    operator
                            + formatSemantics(
                            operator,
                            null,
                            function);

            String opId =
                    createOperatorNode(
                            operator,
                            semantics,
                            "hexagon");

            EDGES.add(new Edge(leftNode, opId, "left"));
            EDGES.add(new Edge(rightNode, opId, "right"));
            EDGES.add(new Edge(opId, outNode, "output"));
            writeDotFile();
        }
    }

    public static void recordBranch(Object upstream, Object[] branches, Object predicates)
    {
        synchronized (LOCK)
        {
            String inNode = ensureNode(upstream, "stream");
            String semantics = "branch" + formatSemantics("branch", null, predicates);
            String opId = createOperatorNode("branch", semantics, "diamond");
            EDGES.add(new Edge(inNode, opId, "input"));

            if (branches != null)
            {
                for (int i = 0; i < branches.length; i++)
                {
                    String out = ensureNode(branches[i], "branchOut" + i);
                    EDGES.add(new Edge(opId, out, "branch[" + i + "]"));
                }
            }
            writeDotFile();
        }
    }

    public static void recordThrough(Object input, Object output, Object topic)
    {
        synchronized (LOCK)
        {
            String in = ensureNode(input, "stream");
            String out = ensureNode(output, "stream");
            String topicNode = ensureTopicNode(normalize(topic));
            EDGES.add(new Edge(in, topicNode, "through-write"));
            EDGES.add(new Edge(topicNode, out, "through-read"));
            writeDotFile();
        }
    }

    public static void recordStateStore(
            Object store)
    {
        synchronized (LOCK)
        {
            ensureStateStoreNode(
                    normalize(store));

            writeDotFile();
        }
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

    public static void writeDotFile()
    {
        synchronized (LOCK)
        {
            String path =
                    System.getProperty(
                            "policy.dsl.dot.path",
                            "build/reports/policy-dsl-topology.dot");

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
                        .append(escape(node.label))
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
        return s
                .replace("\\", "\\\\")
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