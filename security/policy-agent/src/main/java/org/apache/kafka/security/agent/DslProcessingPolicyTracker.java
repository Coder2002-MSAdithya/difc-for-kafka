package org.apache.kafka.security.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.security.agent.policy.AppProcessingPolicy;
import org.apache.kafka.security.agent.policy.ProcessingPolicyEnricher;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Captures a requester's KStreams DSL processing policy (sources, aggregations, DIFC sinks)
 * for grant-time verification before {@code CAN_REMOVE} is approved.
 */
public final class DslProcessingPolicyTracker
{
    private static final Object LOCK = new Object();
    private static final String JSON_PATH_PROP = "policy.dsl.json.path";
    private static final String PRINCIPAL_PROP = "policy.app.principal";
    private static final String SERVICE_PROP = "policy.app.id";

    private static final IdentityHashMap<Object, FlowState> STREAM_STATES = new IdentityHashMap<>();
    private static final LinkedHashSet<String> GLOBAL_SOURCES = new LinkedHashSet<>();
    private static final LinkedHashSet<String> AGGREGATIONS = new LinkedHashSet<>();
    private static final List<SinkBinding> SINKS = new ArrayList<>();
    private static final List<EgressPathBinding> EGRESS_PATHS = new ArrayList<>();
    private static final LinkedHashSet<String> COMPONENTS = new LinkedHashSet<>();

    private static String topologyDigest = "";

    static
    {
        COMPONENTS.add("kafka-streams");
        Runtime.getRuntime().addShutdownHook(
                new Thread(DslProcessingPolicyTracker::writePolicyJsonFile, "dsl-policy-json-writer"));
    }

    private DslProcessingPolicyTracker()
    {
    }

    public static void setTopologyDigest(final String digest)
    {
        synchronized (LOCK)
        {
            topologyDigest = digest == null ? "" : digest;
        }
    }

    public static String canonicalizeTopology(final String raw)
    {
        if (raw == null)
        {
            return "";
        }
        return java.util.Arrays.stream(raw.split("\\R"))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .reduce((a, b) -> a + "|" + b)
                .orElse("");
    }

    public static String sha256Base64(final String input)
    {
        try
        {
            final java.security.MessageDigest digest =
                    java.security.MessageDigest.getInstance("SHA-256");
            final byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return java.util.Base64.getEncoder().encodeToString(hash);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to hash topology for attestation", e);
        }
    }

    public static void recordTopologyDescription(final Object topologyDescription)
    {
        if (topologyDescription == null)
        {
            return;
        }
        final String canonical = canonicalizeTopology(String.valueOf(topologyDescription));
        final String digest = sha256Base64(canonical);
        System.out.println("[POLICY][ATTEST] streams.topology.canonical=" + canonical);
        System.out.println("[POLICY][ATTEST] streams.topology.sha256=" + digest);
        setTopologyDigest(digest);
        writePolicyJsonFile();
    }

    public static void recordSource(final Object returned, final Object topics)
    {
        synchronized (LOCK)
        {
            final FlowState state = stateFor(returned, true);
            for (final String topic : extractTopics(topics))
            {
                GLOBAL_SOURCES.add(topic);
                state.sources.add(topic);
            }
        }
    }

    public static void recordUnary(
            final String operator,
            final Object upstream,
            final Object returned,
            final boolean source)
    {
        recordUnary(operator, upstream, returned, source, null);
    }

    public static void recordUnary(
            final String operator,
            final Object upstream,
            final Object returned,
            final boolean source,
            final Object callback)
    {
        synchronized (LOCK)
        {
            if (returned == null)
            {
                return;
            }
            final FlowState out = stateFor(returned, true);
            if (!source && upstream != null)
            {
                out.inheritFrom(stateFor(upstream, false));
            }
            if (isAggregationOperator(operator))
            {
                AGGREGATIONS.add(normalizeOperator(operator));
            }
            out.operators.add(normalizeOperator(operator));
            appendCallbackProjection(out, operator, callback);
        }
    }

    public static void recordJoin(
            final String operator,
            final Object left,
            final Object right,
            final Object returned)
    {
        recordJoin(operator, left, right, returned, null);
    }

    public static void recordJoin(
            final String operator,
            final Object left,
            final Object right,
            final Object returned,
            final Object joiner)
    {
        synchronized (LOCK)
        {
            if (returned == null)
            {
                return;
            }
            final FlowState out = stateFor(returned, true);
            out.inheritFrom(stateFor(left, false));
            out.inheritFrom(stateFor(right, false));
            final String normalized = normalizeOperator(operator);
            AGGREGATIONS.add(normalized);
            out.operators.add(normalized);
            appendCallbackProjection(out, normalized, joiner);
        }
    }

    public static void recordBranch(final Object upstream, final Object[] branches)
    {
        synchronized (LOCK)
        {
            AGGREGATIONS.add("branch");
            if (branches == null)
            {
                return;
            }
            for (final Object branch : branches)
            {
                if (branch == null)
                {
                    continue;
                }
                final FlowState out = stateFor(branch, true);
                out.inheritFrom(stateFor(upstream, false));
            }
        }
    }

    public static void recordThrough(final Object input, final Object output, final Object topic)
    {
        synchronized (LOCK)
        {
            if (output == null)
            {
                return;
            }
            final FlowState out = stateFor(output, true);
            out.inheritFrom(stateFor(input, false));
            out.sources.add(normalizeTopic(topic));
        }
    }

    public static void recordMapValuesProjection(
            final Object upstream,
            final Object returned,
            final Object mapper)
    {
        synchronized (LOCK)
        {
            refreshCallbackProjection(returned, "mapValues", mapper);
        }
    }

    public static void recordProcessProjection(
            final Object upstream,
            final Object returned,
            final Object processorSupplier)
    {
        synchronized (LOCK)
        {
            refreshCallbackProjection(returned, "process", processorSupplier);
        }
    }

    private static void appendCallbackProjection(
            final FlowState state,
            final String operator,
            final Object callback)
    {
        final org.apache.kafka.security.agent.policy.OperatorCallbackEffect effect =
                org.apache.kafka.security.agent.policy.CallbackProjectionAnalyzer.analyzeEffect(
                        normalizeOperator(operator), callback);
        state.callbackProjections.add(CallbackBinding.fromEffect(normalizeOperator(operator), effect));
        if (!effect.outputFields().isEmpty())
        {
            applyProjectionFields(state, effect.outputFields(), normalizeOperator(operator));
        }
        if (!effect.selectionFields().isEmpty())
        {
            System.out.println(
                    "[POLICY][ATTEST] "
                            + normalizeOperator(operator)
                            + ".selection="
                            + effect.selectionExpression());
        }
    }

    private static void refreshCallbackProjection(
            final Object returned,
            final String operator,
            final Object callback)
    {
        if (returned == null)
        {
            return;
        }
        final FlowState out = stateFor(returned, false);
        final org.apache.kafka.security.agent.policy.OperatorCallbackEffect effect =
                org.apache.kafka.security.agent.policy.CallbackProjectionAnalyzer.analyzeEffect(
                        normalizeOperator(operator), callback);
        if (!out.callbackProjections.isEmpty())
        {
            for (int i = out.callbackProjections.size() - 1; i >= 0; i--)
            {
                final CallbackBinding binding = out.callbackProjections.get(i);
                if (normalizeOperator(operator).equals(binding.operator))
                {
                    binding.applyEffect(effect);
                    if (!effect.outputFields().isEmpty())
                    {
                        applyProjectionFields(out, effect.outputFields(), normalizeOperator(operator));
                    }
                    return;
                }
            }
        }
        out.callbackProjections.add(CallbackBinding.fromEffect(normalizeOperator(operator), effect));
        if (!effect.outputFields().isEmpty())
        {
            applyProjectionFields(out, effect.outputFields(), normalizeOperator(operator));
        }
    }

    private static void applyProjectionFields(
            final FlowState state,
            final java.util.Set<String> projected,
            final String operator)
    {
        state.projectedFields.clear();
        state.projectedFields.addAll(projected);
        System.out.println("[POLICY][ATTEST] " + operator + ".projection=" + projected);
    }

    public static void recordSinkPolicy(final Object stream, final Object topic)
    {
        synchronized (LOCK)
        {
            final String topicName = normalizeTopic(topic);
            if (topicName.isEmpty())
            {
                return;
            }
            final FlowState state = stateFor(stream, false);
            if (!state.projectedFields.isEmpty())
            {
                org.apache.kafka.security.agent.policy.EgressProjectionRegistry.register(
                        topicName, new LinkedHashSet<>(state.projectedFields));
            }
            SINKS.add(new SinkBinding(
                    topicName,
                    new LinkedHashSet<>(state.declassifyTags),
                    new LinkedHashSet<>(state.addTags)));
            EGRESS_PATHS.add(new EgressPathBinding(
                    topicName,
                    new LinkedHashSet<>(state.sources),
                    orderedRelationalOperators(state),
                    relationalCallbackBindings(state),
                    new LinkedHashSet<>(state.declassifyTags),
                    new LinkedHashSet<>(state.addTags)));
        }
    }

    public static void registerComponent(final String component)
    {
        synchronized (LOCK)
        {
            if (component != null && !component.isEmpty())
            {
                COMPONENTS.add(component);
            }
        }
    }

    public static void recordDifcDeclassifyTags(final Object stream, final Object tags, final Object returned)
    {
        synchronized (LOCK)
        {
            final Set<String> parsed = parseTags(tags);
            final FlowState in = stateFor(stream, false);
            in.declassifyTags.addAll(parsed);
            in.operators.add("declassifyTags");
            DslGraphTracker.recordDifcOp("declassifyTags", stream, tags, returned);
            if (returned != null)
            {
                final FlowState out = stateFor(returned, true);
                out.inheritFrom(in);
            }
        }
    }

    public static void recordDifcAddTags(final Object stream, final Object tags, final Object returned)
    {
        synchronized (LOCK)
        {
            final Set<String> parsed = parseTags(tags);
            final FlowState in = stateFor(stream, false);
            in.addTags.addAll(parsed);
            in.operators.add("addTags");
            DslGraphTracker.recordDifcOp("addTags", stream, tags, returned);
            if (returned != null)
            {
                final FlowState out = stateFor(returned, true);
                out.inheritFrom(in);
            }
        }
    }

    public static void writePolicyJsonFile()
    {
        synchronized (LOCK)
        {
            try
            {
                final String principal = System.getProperty(PRINCIPAL_PROP, "");
                final String service = System.getProperty(SERVICE_PROP, "");
                final String generatedAt = Instant.now().toString();
                final String sourcesJson = toJsonStringArray(GLOBAL_SOURCES);
                final String aggregationsJson = toJsonStringArray(AGGREGATIONS);
                final String sinksJson = toJsonSinksArray();
                final String componentsJson = toJsonStringArray(COMPONENTS);
                enrichEgressPathsFromGraph();
                final String egressPathsJson = toJsonEgressPathsArray();
                final String graphJson = DslGraphTracker.exportPolicyGraphJson();
                final String aggregationAnalysisJson = DslGraphTracker.exportAggregationAnalysisJson();

                final String preEnrichCanonical = PolicyAttestationSigner.buildCanonicalPolicyJson(
                        2,
                        generatedAt,
                        topologyDigest,
                        principal,
                        service,
                        componentsJson,
                        sourcesJson,
                        aggregationsJson,
                        sinksJson,
                        egressPathsJson,
                        graphJson,
                        aggregationAnalysisJson);

                final ObjectMapper mapper = new ObjectMapper();
                final AppProcessingPolicy policy =
                        mapper.readValue(preEnrichCanonical, AppProcessingPolicy.class);
                ProcessingPolicyEnricher.enrich(policy);
                final String enrichedAggregationJson =
                        mapper.writeValueAsString(policy.getAggregationAnalysis());
                final String relationalAlgebraAnalysisJson =
                        mapper.writeValueAsString(policy.getRelationalAlgebraAnalysis());

                final String canonical = PolicyAttestationSigner.buildCanonicalPolicyJson(
                        2,
                        generatedAt,
                        topologyDigest,
                        principal,
                        service,
                        componentsJson,
                        sourcesJson,
                        aggregationsJson,
                        sinksJson,
                        egressPathsJson,
                        graphJson,
                        enrichedAggregationJson,
                        relationalAlgebraAnalysisJson);

                final String pathProp = System.getProperty(JSON_PATH_PROP);
                if (pathProp == null || pathProp.isEmpty())
                {
                    System.out.println("[POLICY][ATTEST] processing-policy export skipped (no "
                            + JSON_PATH_PROP + ")");
                    return;
                }

                final Path output = Paths.get(pathProp);
                Files.createDirectories(output.getParent());
                final String envelope = PolicyAttestationSigner.signAndWrap(canonical);
                Files.writeString(output, envelope, StandardCharsets.UTF_8);
                System.out.println("[POLICY][ATTEST] processing-policy written path=" + output);
            }
            catch (Exception e)
            {
                System.err.println("[POLICY][ATTEST] failed to write processing policy: " + e.getMessage());
            }
        }
    }

    private static void enrichEgressPathsFromGraph()
    {
        for (final EgressPathBinding path : EGRESS_PATHS)
        {
            if (path.ingressTopics.isEmpty())
            {
                path.ingressTopics.addAll(DslGraphTracker.deriveIngressTopicsForEgressTopic(path.topic));
            }
        }
    }

    private static FlowState stateFor(final Object stream, final boolean create)
    {
        FlowState state = STREAM_STATES.get(stream);
        if (state == null && create)
        {
            state = new FlowState();
            STREAM_STATES.put(stream, state);
        }
        return state == null ? new FlowState() : state;
    }

    private static boolean isAggregationOperator(final String operator)
    {
        if (operator == null)
        {
            return false;
        }
        return switch (operator)
        {
            case "aggregate", "reduce", "count", "join", "leftJoin", "outerJoin", "merge",
                 "branch", "split", "groupBy", "groupByKey", "windowedBy" ->
                    true;
            default -> false;
        };
    }

    private static String normalizeOperator(final String operator)
    {
        if ("leftJoin".equals(operator) || "outerJoin".equals(operator))
        {
            return "join";
        }
        return operator;
    }

    private static List<String> extractTopics(final Object topics)
    {
        final List<String> result = new ArrayList<>();
        if (topics == null)
        {
            return result;
        }
        if (topics instanceof Collection<?> collection)
        {
            for (final Object topic : collection)
            {
                addTopic(result, topic);
            }
            return result;
        }
        if (topics instanceof String[] array)
        {
            for (final String topic : array)
            {
                addTopic(result, topic);
            }
            return result;
        }
        addTopic(result, topics);
        return result;
    }

    private static void addTopic(final List<String> result, final Object topic)
    {
        final String normalized = normalizeTopic(topic);
        if (!normalized.isEmpty())
        {
            result.add(normalized);
        }
    }

    private static String normalizeTopic(final Object topic)
    {
        if (topic == null)
        {
            return "";
        }
        String value = String.valueOf(topic).trim();
        if (value.startsWith("[") && value.endsWith("]"))
        {
            value = value.substring(1, value.length() - 1).trim();
        }
        if (value.contains("StaticTopicNameExtractor"))
        {
            final int start = value.indexOf('(');
            final int end = value.lastIndexOf(')');
            if (start >= 0 && end > start)
            {
                value = value.substring(start + 1, end).trim();
            }
        }
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2)
        {
            value = value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static Set<String> parseTags(final Object tags)
    {
        final LinkedHashSet<String> parsed = new LinkedHashSet<>();
        if (tags == null)
        {
            return parsed;
        }
        if (tags instanceof Collection<?> collection)
        {
            for (final Object tag : collection)
            {
                if (tag != null)
                {
                    parsed.add(String.valueOf(tag));
                }
            }
            return parsed;
        }
        if (tags instanceof String[] array)
        {
            for (final String tag : array)
            {
                if (tag != null && !tag.isEmpty())
                {
                    parsed.add(tag);
                }
            }
            return parsed;
        }
        final String raw = String.valueOf(tags);
        for (final String part : raw.replace("[", "").replace("]", "").split(","))
        {
            final String trimmed = part.trim();
            if (!trimmed.isEmpty())
            {
                parsed.add(trimmed);
            }
        }
        return parsed;
    }

    private static String toJsonStringArray(final Collection<String> values)
    {
        final StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (final String value : new TreeSet<>(values))
        {
            if (!first)
            {
                sb.append(',');
            }
            sb.append('"').append(escapeJson(value)).append('"');
            first = false;
        }
        sb.append(']');
        return sb.toString();
    }

    private static String toJsonEgressPathsArray()
    {
        final StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (final EgressPathBinding path : EGRESS_PATHS)
        {
            if (!first)
            {
                sb.append(',');
            }
            sb.append('{')
                    .append("\"topic\":\"").append(escapeJson(path.topic)).append("\",")
                    .append("\"ingressTopics\":").append(toJsonStringArray(path.ingressTopics)).append(',')
                    .append("\"operators\":").append(toJsonStringArray(path.operators)).append(',')
                    .append("\"callbackProjections\":")
                    .append(toJsonCallbackProjectionsArray(path.callbackProjections)).append(',')
                    .append("\"declassifyTags\":").append(toJsonStringArray(path.declassifyTags)).append(',')
                    .append("\"addTags\":").append(toJsonStringArray(path.addTags))
                    .append('}');
            first = false;
        }
        sb.append(']');
        return sb.toString();
    }

    private static String toJsonCallbackProjectionsArray(final List<CallbackBinding> bindings)
    {
        final StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (final CallbackBinding binding : bindings)
        {
            if (!first)
            {
                sb.append(',');
            }
            sb.append('{')
                    .append("\"operator\":\"").append(escapeJson(binding.operator)).append("\",")
                    .append("\"outputFields\":").append(toJsonStringArray(binding.outputFields)).append(',')
                    .append("\"selectionFields\":").append(toJsonStringArray(binding.selectionFields)).append(',')
                    .append("\"selectionExpression\":\"").append(escapeJson(binding.selectionExpression)).append("\",")
                    .append("\"keyFields\":").append(toJsonStringArray(binding.keyFields))
                    .append('}');
            first = false;
        }
        sb.append(']');
        return sb.toString();
    }

    private static LinkedHashSet<String> relationalOperators(final FlowState state)
    {
        final LinkedHashSet<String> relational = new LinkedHashSet<>();
        for (final String operator : state.operators)
        {
            final String normalized = org.apache.kafka.security.agent.policy.RelationalAlgebraTreeSupport.normalizeOp(
                    operator + "()");
            if (!org.apache.kafka.security.agent.policy.RelationalAlgebraTreeSupport.isPassthroughOp(normalized))
            {
                relational.add(operator);
            }
        }
        return relational;
    }

    private static List<String> orderedRelationalOperators(final FlowState state)
    {
        final List<String> relational = new ArrayList<>();
        for (final String operator : state.operators)
        {
            final String normalized = org.apache.kafka.security.agent.policy.RelationalAlgebraTreeSupport.normalizeOp(
                    operator + "()");
            if (!org.apache.kafka.security.agent.policy.RelationalAlgebraTreeSupport.isPassthroughOp(normalized))
            {
                relational.add(operator);
            }
        }
        return relational;
    }

    private static List<CallbackBinding> relationalCallbackBindings(final FlowState state)
    {
        final List<CallbackBinding> relational = new ArrayList<>();
        int searchFrom = 0;
        for (final String operator : state.operators)
        {
            final String normalized = org.apache.kafka.security.agent.policy.RelationalAlgebraTreeSupport.normalizeOp(
                    operator + "()");
            if (org.apache.kafka.security.agent.policy.RelationalAlgebraTreeSupport.isPassthroughOp(normalized))
            {
                continue;
            }
            CallbackBinding matched = null;
            for (int i = searchFrom; i < state.callbackProjections.size(); i++)
            {
                final CallbackBinding candidate = state.callbackProjections.get(i);
                if (operator.equals(candidate.operator))
                {
                    matched = candidate;
                    searchFrom = i + 1;
                    break;
                }
            }
            relational.add(
                    matched != null
                            ? matched
                            : CallbackBinding.fromEffect(operator, org.apache.kafka.security.agent.policy.OperatorCallbackEffect.empty()));
        }
        return relational;
    }

    private static String toJsonSinksArray()
    {
        final StringBuilder sb = new StringBuilder("[");
        boolean firstSink = true;
        for (final SinkBinding sink : SINKS)
        {
            if (!firstSink)
            {
                sb.append(',');
            }
            sb.append('{')
                    .append("\"topic\":\"").append(escapeJson(sink.topic)).append("\",")
                    .append("\"declassifyTags\":").append(toJsonStringArray(sink.declassifyTags)).append(',')
                    .append("\"addTags\":").append(toJsonStringArray(sink.addTags))
                    .append('}');
            firstSink = false;
        }
        sb.append(']');
        return sb.toString();
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

    private static final class FlowState
    {
        private final LinkedHashSet<String> sources = new LinkedHashSet<>();
        private final List<String> operators = new ArrayList<>();
        private final LinkedHashSet<String> declassifyTags = new LinkedHashSet<>();
        private final LinkedHashSet<String> addTags = new LinkedHashSet<>();
        private final LinkedHashSet<String> projectedFields = new LinkedHashSet<>();
        private final List<CallbackBinding> callbackProjections = new ArrayList<>();

        private void inheritFrom(final FlowState other)
        {
            sources.addAll(other.sources);
            operators.addAll(other.operators);
            declassifyTags.addAll(other.declassifyTags);
            addTags.addAll(other.addTags);
            callbackProjections.addAll(other.callbackProjections);
            if (!other.projectedFields.isEmpty())
            {
                projectedFields.clear();
                projectedFields.addAll(other.projectedFields);
            }
        }
    }

    private static final class CallbackBinding
    {
        private final String operator;
        private final LinkedHashSet<String> outputFields;
        private final LinkedHashSet<String> selectionFields;
        private String selectionExpression;
        private final LinkedHashSet<String> keyFields;

        private CallbackBinding(
                final String operator,
                final LinkedHashSet<String> outputFields,
                final LinkedHashSet<String> selectionFields,
                final String selectionExpression,
                final LinkedHashSet<String> keyFields)
        {
            this.operator = operator == null ? "" : operator;
            this.outputFields = outputFields == null ? new LinkedHashSet<>() : outputFields;
            this.selectionFields = selectionFields == null ? new LinkedHashSet<>() : selectionFields;
            this.selectionExpression = selectionExpression == null ? "" : selectionExpression;
            this.keyFields = keyFields == null ? new LinkedHashSet<>() : keyFields;
        }

        private static CallbackBinding fromEffect(
                final String operator,
                final org.apache.kafka.security.agent.policy.OperatorCallbackEffect effect)
        {
            return new CallbackBinding(
                    operator,
                    new LinkedHashSet<>(effect.outputFields()),
                    new LinkedHashSet<>(effect.selectionFields()),
                    effect.selectionExpression(),
                    new LinkedHashSet<>(effect.keyFields()));
        }

        private void applyEffect(final org.apache.kafka.security.agent.policy.OperatorCallbackEffect effect)
        {
            outputFields.clear();
            outputFields.addAll(effect.outputFields());
            selectionFields.clear();
            selectionFields.addAll(effect.selectionFields());
            selectionExpression = effect.selectionExpression();
            keyFields.clear();
            keyFields.addAll(effect.keyFields());
        }
    }

    private record EgressPathBinding(
            String topic,
            LinkedHashSet<String> ingressTopics,
            List<String> operators,
            List<CallbackBinding> callbackProjections,
            LinkedHashSet<String> declassifyTags,
            LinkedHashSet<String> addTags)
    {
        private EgressPathBinding
        {
            topic = topic == null ? "" : topic;
        }
    }

    private record SinkBinding(String topic, LinkedHashSet<String> declassifyTags, LinkedHashSet<String> addTags)
    {
        private SinkBinding
        {
            topic = topic == null ? "" : topic;
        }
    }
}
