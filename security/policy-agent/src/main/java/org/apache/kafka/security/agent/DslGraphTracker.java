package org.apache.kafka.security.agent;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
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
    private static final LinkedHashMap<String, Node> NODES = new LinkedHashMap<>();
    private static final List<Edge> EDGES = new ArrayList<>();
    private static int nodeSeq = 1;
    private static final boolean DEBUG_LAMBDAS = true;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(DslGraphTracker::writeDotFile, "dsl-graph-dot-writer"));
    }

    private DslGraphTracker() {}

    public static void recordSource(Object returned, Object topics) {
        recordUnary("from", null, returned, normalize(topics), null, true, false);
    }

    public static void recordUnary(String operator, Object upstream, Object returned, Object arg, Object function, boolean source, boolean sink) {
        synchronized (LOCK) {
            String inNode = source ? null : ensureNode(upstream, "stream");
            String outNode = sink ? ensureNode(upstream, "stream") : ensureNode(returned, operator + "Out");
            String opId = "op" + (nodeSeq++);
            String semantics = operator + formatSemantics(operator, arg, function);
            Node opNode = new Node(opId, "operator", semantics, false);
            NODES.put(opId, opNode);
            if (inNode != null) EDGES.add(new Edge(inNode, opId, "input"));
            if (!sink) {
                EDGES.add(new Edge(opId, outNode, "output"));
            } else {
                String sinkId = "sink" + (nodeSeq++);
                Node sinkNode = new Node(sinkId, "sink", "to(" + normalize(arg) + ")", false);
                NODES.put(sinkId, sinkNode);
                EDGES.add(new Edge(opId, sinkId, "writes"));
            }
            writeDotFile();
        }
    }

    private static String ensureNode(Object ref, String fallback) {
        if (ref == null) {
            String n = "anon" + (nodeSeq++);
            NODES.put(n, new Node(n, "stream", fallback, true));
            return n;
        }
        String existing = OBJECT_NODE.get(ref);
        if (existing != null) return existing;
        String id = "n" + (nodeSeq++);
        String label = fallback + "\n" + ref.getClass().getSimpleName();
        boolean synthetic = fallback.endsWith("Out") || "stream".equals(fallback);
        NODES.put(id, new Node(id, "stream", label, synthetic));
        OBJECT_NODE.put(ref, id);
        return id;
    }

    public static void recordJoin(String operator, Object left, Object right, Object returned, Object function) {
        synchronized (LOCK) {
            String leftNode = ensureNode(left, "stream");
            String rightNode = ensureNode(right, "stream");
            String outNode = ensureNode(returned, operator + "Out");

            String opId = "op" + (nodeSeq++);
            String semantics = operator + formatSemantics(operator, null, function);
            NODES.put(opId, new Node(opId, "operator", semantics, false));

            EDGES.add(new Edge(leftNode, opId, "left"));
            EDGES.add(new Edge(rightNode, opId, "right"));
            EDGES.add(new Edge(opId, outNode, "output"));
            writeDotFile();
        }
    }

    public static void recordBranch(Object upstream, Object[] branches, Object predicates) {
        synchronized (LOCK) {
            String inNode = ensureNode(upstream, "stream");
            String opId = "op" + (nodeSeq++);
            String semantics = "branch" + formatSemantics("branch", null, predicates);
            NODES.put(opId, new Node(opId, "operator", semantics, false));
            EDGES.add(new Edge(inNode, opId, "input"));

            if (branches != null) {
                for (int i = 0; i < branches.length; i++) {
                    String out = ensureNode(branches[i], "branchOut" + i);
                    EDGES.add(new Edge(opId, out, "branch[" + i + "]"));
                }
            }
            writeDotFile();
        }
    }

    private static String formatSemantics(String operator, Object arg, Object function) {
        List<String> details = new ArrayList<>();
        if (arg != null) details.add(normalize(arg));
        if (function != null) details.add(renderLambda(operator, function));
        return details.isEmpty() ? "()" : "(" + String.join(", ", details) + ")";
    }

    private static String renderLambda(String operator, Object fn) {
        SerializedLambda sl = toSerializedLambda(fn);
        if (sl != null) {
            debug("serialized-lambda hit", operator, fn.getClass().getName(), sl.getImplClass() + "::" + sl.getImplMethodName());
            return renderLambda(operator, sl.getImplClass(), sl.getImplMethodName(), sl.getImplMethodSignature());
        }

        LambdaRegistry.LambdaInfo info = LambdaRegistry.lookup(fn.getClass());
        if (info != null) {
            debug("registry hit", operator, fn.getClass().getName(), info.implClass + "::" + info.implMethod);
            return renderLambda(operator, info.implClass.replace('.', '/'), info.implMethod, info.implDesc);
        }

        debug("fallback fn class", operator, fn.getClass().getName(), "no serialized/registry match");
        return "fn=" + fn.getClass().getName();
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
                if (mi.owner.equals("java/lang/Integer") && mi.name.equals("parseInt") && !stack.isEmpty())
                {
                    String v = stack.pop();
                    stack.push("Integer.parseInt(" + v + ")");
                    continue;
                }
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

    public static void writeDotFile() {
        synchronized (LOCK) {
            String path = System.getProperty("policy.dsl.dot.path", "build/reports/policy-dsl-topology.dot");
            Path dotPath = Paths.get(path).toAbsolutePath();
            StringBuilder sb = new StringBuilder();
            sb.append("digraph KafkaDsl {\n");
            sb.append("  rankdir=LR;\n  node [shape=box, style=rounded];\n");
            sb.append("  subgraph cluster_legend {\n");
            sb.append("    label=\"Legend\";\n");
            sb.append("    keySynthetic [label=\"Synthetic stream node\", style=\"rounded,filled\", fillcolor=\"#FFE9A8\"];\n");
            sb.append("    keyNormal [label=\"Operator/Sink node\", style=\"rounded,filled\", fillcolor=\"#E8F0FE\"];\n");
            sb.append("  }\n");
            for (Node node : NODES.values()) {
                String style = node.synthetic ? "rounded,filled" : "rounded";
                String fillColor = node.synthetic ? "#FFE9A8" : "#E8F0FE";
                sb.append("  ").append(node.id)
                        .append(" [label=\"").append(escape(node.label))
                        .append("\", class=\"").append(node.kind)
                        .append("\", style=\"").append(style)
                        .append("\", fillcolor=\"").append(fillColor)
                        .append("\"];\n");
            }
            for (Edge edge : EDGES) {
                sb.append("  ").append(edge.from).append(" -> ").append(edge.to).append(" [label=\"").append(escape(edge.label)).append("\"];\n");
            }
            sb.append("  graph [label=\"Generated at ").append(Instant.now()).append("\"];\n}");
            try {
                Path parent = dotPath.getParent();
                if (parent != null)
                {
                    Files.createDirectories(parent);
                }

                Files.writeString(dotPath, sb.toString(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static String escape(String s) { return s.replace("\\", "\\\\").replace("\"", "\\\""); }

    private record Node(String id, String kind, String label, boolean synthetic) {}
    private record Edge(String from, String to, String label) {}
}