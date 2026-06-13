package org.apache.kafka.security.agent;

import org.apache.kafka.security.agent.policy.AppProcessingPolicy;
import org.apache.kafka.security.agent.policy.ProcessingPolicyGraph;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Captures application-level {@code KafkaProducer}/{@code KafkaConsumer} usage outside Kafka Streams
 * internal clients (REST handlers, standalone producers, plain consumers).
 */
public final class AppClientPolicyTracker {

    private static final Object LOCK = new Object();
    private static final String COMPONENT_ID = "comp_app_client";
    private static final LinkedHashSet<String> COMPONENTS = new LinkedHashSet<>();
    private static final LinkedHashSet<String> CONSUMER_SOURCES = new LinkedHashSet<>();
    private static final List<ProducerSendBinding> PRODUCER_SENDS = new ArrayList<>();
    private static boolean restIngressObserved = false;

    public static void clearStateForTests() {
        synchronized (LOCK) {
            COMPONENTS.clear();
            CONSUMER_SOURCES.clear();
            PRODUCER_SENDS.clear();
            restIngressObserved = false;
        }
    }

    private AppClientPolicyTracker() {}

    public static void recordProducerSend(final Object record) {
        if (record == null || insideStreamsInternal()) {
            return;
        }
        final String topic = topicName(record);
        if (topic.isEmpty()) {
            return;
        }
        recordProducerSendBinding(
                topic,
                producerOperators(record),
                headerTags(record, "tags"),
                headerTags(record, "declassify"),
                stackIndicatesRestIngress());
    }

    public static void recordProducerSendBinding(
            final String topic,
            final List<String> operators,
            final Set<String> addTags,
            final Set<String> declassifyTags,
            final boolean restIngress) {
        if (topic == null || topic.isEmpty() || insideStreamsInternal()) {
            return;
        }
        synchronized (LOCK) {
            COMPONENTS.add("kafka-producer");
            if (restIngress) {
                COMPONENTS.add("rest");
                restIngressObserved = true;
            }
            PRODUCER_SENDS.add(new ProducerSendBinding(topic, operators, addTags, declassifyTags));
            System.out.printf(
                    "[POLICY][CLIENT] producer.send topic=%s operators=%s addTags=%s declassifyTags=%s rest=%s%n",
                    topic,
                    operators,
                    addTags,
                    declassifyTags,
                    restIngress);
        }
    }

    public static void recordConsumerSubscribe(final Object topicsArg) {
        if (topicsArg == null || insideStreamsInternal()) {
            return;
        }
        synchronized (LOCK) {
            final Set<String> topics = extractTopicNames(topicsArg);
            if (topics.isEmpty()) {
                return;
            }
            COMPONENTS.add("kafka-consumer");
            CONSUMER_SOURCES.addAll(topics);
            System.out.printf("[POLICY][CLIENT] consumer.subscribe topics=%s%n", topics);
        }
    }

    public static void recordConsumerAssign(final Object partitionsArg) {
        if (partitionsArg == null || insideStreamsInternal()) {
            return;
        }
        synchronized (LOCK) {
            final Set<String> topics = topicNamesFromPartitions(partitionsArg);
            if (topics.isEmpty()) {
                return;
            }
            COMPONENTS.add("kafka-consumer");
            CONSUMER_SOURCES.addAll(topics);
            System.out.printf("[POLICY][CLIENT] consumer.assign topics=%s%n", topics);
        }
    }

    /**
     * Merges captured app-client bindings into the exported processing policy before RA analysis.
     */
    public static void mergeInto(final AppProcessingPolicy policy) {
        if (policy == null) {
            return;
        }
        synchronized (LOCK) {
            if (COMPONENTS.isEmpty() && PRODUCER_SENDS.isEmpty() && CONSUMER_SOURCES.isEmpty()) {
                return;
            }
            final Set<String> components = new LinkedHashSet<>(policy.getComponents());
            components.addAll(COMPONENTS);
            policy.setComponents(new ArrayList<>(components));

            final Set<String> sources = new LinkedHashSet<>(policy.getSources());
            sources.addAll(CONSUMER_SOURCES);
            policy.setSources(new ArrayList<>(sources));

            final Map<String, AppProcessingPolicy.EgressPath> egressByTopic = new LinkedHashMap<>();
            for (final AppProcessingPolicy.EgressPath existing : policy.getEgressPaths()) {
                mergeEgressPath(egressByTopic, existing);
            }
            final List<AppProcessingPolicy.SinkPolicy> sinks = new ArrayList<>(policy.getSinks());

            final ProcessingPolicyGraph graph = policy.getGraph();
            ensureGraphNode(
                    graph,
                    COMPONENT_ID,
                    "component",
                    restIngressObserved ? "application (REST/producer)" : "application (kafka-client)",
                    null);

            for (final String sourceTopic : CONSUMER_SOURCES) {
                final String topicId = ensureTopicGraphNode(graph, sourceTopic);
                ensureGraphEdge(graph, topicId, COMPONENT_ID, "ingress");
            }

            for (final ProducerSendBinding send : PRODUCER_SENDS) {
                mergeProducerSend(policy, graph, egressByTopic, sinks, send);
            }

            policy.setEgressPaths(new ArrayList<>(egressByTopic.values()));
            policy.setSinks(sinks);
            policy.setGraph(graph);
        }
    }

    private static void mergeProducerSend(
            final AppProcessingPolicy policy,
            final ProcessingPolicyGraph graph,
            final Map<String, AppProcessingPolicy.EgressPath> egressByTopic,
            final List<AppProcessingPolicy.SinkPolicy> sinks,
            final ProducerSendBinding send) {
        final AppProcessingPolicy.EgressPath path = new AppProcessingPolicy.EgressPath();
        path.setTopic(send.topic());
        path.setIngressTopics(new ArrayList<>(CONSUMER_SOURCES));
        path.setOperators(new ArrayList<>(send.operators()));
        path.setAddTags(new ArrayList<>(send.addTags()));
        path.setDeclassifyTags(new ArrayList<>(send.declassifyTags()));
        mergeEgressPath(egressByTopic, path);

        final AppProcessingPolicy.SinkPolicy sink = new AppProcessingPolicy.SinkPolicy();
        sink.setTopic(send.topic());
        sink.setAddTags(new ArrayList<>(send.addTags()));
        sink.setDeclassifyTags(new ArrayList<>(send.declassifyTags()));
        sinks.add(sink);

        String previous = COMPONENT_ID;
        for (int i = 0; i < send.operators().size(); i++) {
            final String opId = COMPONENT_ID + "_producer_op_" + i;
            ensureGraphNode(graph, opId, "operator", send.operators().get(i) + "()", null);
            ensureGraphEdge(graph, previous, opId, i == 0 ? "ingress" : "input");
            previous = opId;
        }
        ensureGraphEdge(graph, previous, ensureTopicGraphNode(graph, send.topic()), "writes");
    }

    private static void mergeEgressPath(
            final Map<String, AppProcessingPolicy.EgressPath> egressByTopic,
            final AppProcessingPolicy.EgressPath path) {
        if (path.getTopic() == null || path.getTopic().isEmpty()) {
            return;
        }
        final AppProcessingPolicy.EgressPath merged =
                egressByTopic.computeIfAbsent(path.getTopic(), t -> {
                    final AppProcessingPolicy.EgressPath created = new AppProcessingPolicy.EgressPath();
                    created.setTopic(t);
                    return created;
                });
        merged.setIngressTopics(union(merged.getIngressTopics(), path.getIngressTopics()));
        merged.setOperators(union(merged.getOperators(), path.getOperators()));
        merged.setDeclassifyTags(union(merged.getDeclassifyTags(), path.getDeclassifyTags()));
        merged.setAddTags(union(merged.getAddTags(), path.getAddTags()));
    }

    private static List<String> union(final List<String> left, final List<String> right) {
        final LinkedHashSet<String> merged = new LinkedHashSet<>(left);
        merged.addAll(right);
        return new ArrayList<>(merged);
    }

    private static String ensureTopicGraphNode(final ProcessingPolicyGraph graph, final String topic) {
        for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
            if ("topic".equals(node.getKind()) && topic.equals(node.getTopic())) {
                return node.getId();
            }
        }
        final String id = "topic_" + topic.replaceAll("[^A-Za-z0-9_\\-]", "_");
        ensureGraphNode(graph, id, "topic", "topic:" + topic, topic);
        return id;
    }

    private static void ensureGraphNode(
            final ProcessingPolicyGraph graph,
            final String id,
            final String kind,
            final String label,
            final String topic) {
        for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
            if (id.equals(node.getId())) {
                return;
            }
        }
        final ProcessingPolicyGraph.GraphNode node = new ProcessingPolicyGraph.GraphNode();
        node.setId(id);
        node.setKind(kind);
        node.setLabel(label);
        node.setTopic(topic);
        graph.getNodes().add(node);
    }

    private static void ensureGraphEdge(
            final ProcessingPolicyGraph graph,
            final String from,
            final String to,
            final String label) {
        for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
            if (from.equals(edge.getFrom()) && to.equals(edge.getTo()) && label.equals(edge.getLabel())) {
                return;
            }
        }
        final ProcessingPolicyGraph.GraphEdge edge = new ProcessingPolicyGraph.GraphEdge();
        edge.setFrom(from);
        edge.setTo(to);
        edge.setLabel(label);
        graph.getEdges().add(edge);
    }

    private static Set<String> extractTopicNames(final Object topicsArg) {
        final LinkedHashSet<String> topics = new LinkedHashSet<>();
        if (topicsArg instanceof Collection<?> collection) {
            for (final Object topic : collection) {
                addTopicName(topics, topic);
            }
        } else if (topicsArg instanceof String[] array) {
            for (final String topic : array) {
                addTopicName(topics, topic);
            }
        } else {
            addTopicName(topics, topicsArg);
        }
        return topics;
    }

    private static Set<String> topicNamesFromPartitions(final Object partitionsArg) {
        final LinkedHashSet<String> topics = new LinkedHashSet<>();
        if (!(partitionsArg instanceof Collection<?> collection)) {
            return topics;
        }
        for (final Object partition : collection) {
            final Object topic = invoke(partition, "topic");
            addTopicName(topics, topic);
        }
        return topics;
    }

    private static void addTopicName(final Set<String> topics, final Object topic) {
        if (topic == null) {
            return;
        }
        final String normalized = String.valueOf(topic).trim();
        if (!normalized.isEmpty()) {
            topics.add(normalized);
        }
    }

    private static Set<String> headerTags(final Object record, final String headerName) {
        final LinkedHashSet<String> tags = new LinkedHashSet<>();
        final Object headers = invoke(record, "headers");
        if (headers == null) {
            return tags;
        }
        try {
            final Method lastHeader = headers.getClass().getMethod("lastHeader", String.class);
            final Object header = lastHeader.invoke(headers, headerName);
            if (header == null) {
                return tags;
            }
            final Object value = invoke(header, "value");
            if (value instanceof byte[] bytes) {
                for (final String part : new String(bytes, StandardCharsets.UTF_8).split(":")) {
                    final String trimmed = part.trim();
                    if (!trimmed.isEmpty()) {
                        tags.add(trimmed);
                    }
                }
            }
        } catch (final ReflectiveOperationException ignored) {
            return tags;
        }
        return tags;
    }

    private static boolean stackIndicatesRestIngress() {
        for (final StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            final String cn = frame.getClassName();
            if (cn.startsWith("jakarta.ws.rs")
                    || cn.startsWith("javax.ws.rs")
                    || cn.startsWith("org.glassfish.jersey")
                    || cn.startsWith("org.eclipse.jetty")
                    || cn.contains("MicroserviceUtils")
                    || cn.endsWith("OrdersService")) {
                return true;
            }
        }
        return false;
    }

    private static boolean insideStreamsInternal() {
        try {
            final Class<?> bootstrap =
                    Class.forName("org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap");
            final Method method = bootstrap.getMethod("insideStreamsInternal");
            return Boolean.TRUE.equals(method.invoke(null));
        } catch (final ReflectiveOperationException e) {
            return false;
        }
    }

    private static String topicName(final Object record) {
        Object value = invoke(record, "topic");
        if (value == null) {
            value = invoke(record, "getTopic");
        }
        return stringOrEmpty(value);
    }

    private static List<String> producerOperators(final Object record) {
        final List<String> operators = new ArrayList<>();
        operators.add("producer");
        final Set<String> addTags = headerTags(record, "tags");
        final Set<String> declassifyTags = headerTags(record, "declassify");
        if (!addTags.isEmpty() || !declassifyTags.isEmpty()) {
            operators.add("sendWithTags");
        } else {
            operators.add("send");
        }
        return operators;
    }

    private static Object invoke(final Object target, final String methodName) {
        if (target == null) {
            return null;
        }
        try {
            final Method method = target.getClass().getMethod(methodName);
            return method.invoke(target);
        } catch (final ReflectiveOperationException e) {
            return null;
        }
    }

    private static String stringOrEmpty(final Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private record ProducerSendBinding(
            String topic,
            List<String> operators,
            Set<String> addTags,
            Set<String> declassifyTags) {}
}
