package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.List;

/**
 * Hand-authored supplements for gaps not observed at runtime (disabled when
 * {@code -Dpolicy.manifest.supplement=false}). Runtime capture via {@link org.apache.kafka.security.agent.AppClientPolicyTracker}
 * and Kafka Streams instrumentation is preferred.
 */
public final class PolicyManifestRegistry {

    private PolicyManifestRegistry() {
    }

    public static ProcessingPolicyDocument ordersProducer() {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId("OrdersService");
        doc.setPrincipal("orders-svc");

        final ProcessingPolicyDocument.SinkBinding ordersEgress = new ProcessingPolicyDocument.SinkBinding();
        ordersEgress.setEgressTopic("orders");
        ordersEgress.setIngressTopics(List.of());
        ordersEgress.setAddedTags(List.of("order"));
        ordersEgress.setRemovedTags(List.of());
        ordersEgress.setOperators(List.of("producer", "sendWithTags"));
        doc.setSinkBindings(List.of(ordersEgress));

        final TagFlowPolicy orderFlow = new TagFlowPolicy();
        orderFlow.setTag("order");
        orderFlow.setIngressTopics(List.of());
        orderFlow.setEgressTopics(List.of("orders"));
        orderFlow.setRemovedOnEgress(false);
        orderFlow.setAggregators(List.of());
        doc.setTagFlows(List.of(orderFlow));
        return doc;
    }

    public static ProcessingPolicyDocument emailConsumer() {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId("EmailService");
        doc.setPrincipal("email-svc");

        final ProcessingPolicyDocument.SinkBinding enriched = new ProcessingPolicyDocument.SinkBinding();
        enriched.setEgressTopic("orders-enriched");
        enriched.setIngressTopics(List.of("orders", "payments", "customers"));
        enriched.setOperators(List.of("stream", "join"));
        doc.setSinkBindings(List.of(enriched));

        final TagFlowPolicy orderFlow = new TagFlowPolicy();
        orderFlow.setTag("order");
        orderFlow.setIngressTopics(List.of("orders"));
        orderFlow.setEgressTopics(List.of("orders-enriched"));
        orderFlow.setRemovedOnEgress(false);
        orderFlow.setAggregators(List.of("join"));
        doc.setTagFlows(List.of(orderFlow));
        return doc;
    }

    public static ProcessingPolicyDocument fraudValidator() {
        return validatorSink(
            "FraudService",
            "fraud-svc",
            "fraud",
            List.of("filter", "groupBy", "windowedBy", "aggregate", "split", "mapValues", "merge"));
    }

    public static ProcessingPolicyDocument inventoryValidator() {
        final ProcessingPolicyDocument doc = validatorSink(
            "InventoryService",
            "inventory-svc",
            "inv-valid",
            List.of("selectKey", "filter", "join", "mapValues", "process"));
        doc.setTableSources(List.of("warehouse-inventory"));
        return doc;
    }

    public static ProcessingPolicyDocument orderDetailsValidator() {
        return validatorSink(
            "OrderDetailsService",
            "order-details-svc",
            "order-valid",
            List.of("filter", "mapValues"));
    }

    private static ProcessingPolicyDocument validatorSink(
            final String applicationId,
            final String principal,
            final String validationTag,
            final List<String> operators) {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId(applicationId);
        doc.setPrincipal(principal);

        final ProcessingPolicyDocument.SinkBinding validationsOut = new ProcessingPolicyDocument.SinkBinding();
        validationsOut.setEgressTopic("order-validations");
        validationsOut.setIngressTopics(List.of("orders"));
        validationsOut.setRemovedTags(List.of("order"));
        validationsOut.setAddedTags(List.of(validationTag));
        validationsOut.setOperators(operators);
        validationsOut.setCallbackProjections(validationMapValuesProjection(operators));
        doc.setSinkBindings(List.of(validationsOut));
        return doc;
    }

    private static List<ProcessingPolicyDocument.CallbackProjectionBinding> validationMapValuesProjection(
            final List<String> operators) {
        final List<ProcessingPolicyDocument.CallbackProjectionBinding> callbacks = new ArrayList<>();
        for (final String operator : operators) {
            if (!"mapValues".equals(operator)) {
                continue;
            }
            final ProcessingPolicyDocument.CallbackProjectionBinding binding =
                    new ProcessingPolicyDocument.CallbackProjectionBinding();
            binding.setOperator(operator);
            binding.setOutputFields(List.of("orderId", "checkType", "validationResult"));
            callbacks.add(binding);
        }
        return callbacks;
    }

    public static ProcessingPolicyDocument validationsAggregator() {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId("ValidationsAggregatorService");
        doc.setPrincipal("validations-agg-svc");

        final ProcessingPolicyDocument.SinkBinding ordersOut = new ProcessingPolicyDocument.SinkBinding();
        ordersOut.setEgressTopic("orders");
        ordersOut.setIngressTopics(List.of("order-validations", "orders"));
        ordersOut.setRemovedTags(List.of("fraud", "inv-valid", "order-valid", "order"));
        ordersOut.setOperators(List.of("stream", "aggregate", "join", "merge", "declassifyTags", "to"));
        doc.setSinkBindings(List.of(ordersOut));

        final TagFlowPolicy orderFlow = new TagFlowPolicy();
        orderFlow.setTag("order");
        orderFlow.setIngressTopics(List.of("orders"));
        orderFlow.setEgressTopics(List.of("orders"));
        orderFlow.setRemovedOnEgress(true);
        orderFlow.setAggregators(List.of("aggregate", "join", "merge"));
        doc.setTagFlows(List.of(orderFlow));
        return doc;
    }

    public static ProcessingPolicyDocument stockRepublisher() {
        return kafkaClientRepublisher(
                "StockService",
                "stock-svc",
                "ORDER_EVENT_TOPIC",
                "STOCK_CHECK_EVENT_TOPIC",
                List.of("order"),
                List.of("stock", "card"),
                List.of("producer", "sendWithTags", "forStockCheck"));
    }

    public static ProcessingPolicyDocument validationRepublisher() {
        return kafkaClientRepublisher(
                "ValidationService",
                "validation-svc",
                "STOCK_CHECK_EVENT_TOPIC",
                "VALIDATION_EVENT_TOPIC",
                List.of("stock", "card"),
                List.of("validation"),
                List.of("producer", "sendWithTags", "forValidation"));
    }

    public static ProcessingPolicyDocument paymentRepublisher() {
        return kafkaClientRepublisher(
                "PaymentService",
                "payment-svc",
                "VALIDATION_EVENT_TOPIC",
                "BILLING_EVENT_TOPIC",
                List.of("validation"),
                List.of("billing"),
                List.of("producer", "sendWithTags", "forBilling"));
    }

    private static ProcessingPolicyDocument kafkaClientRepublisher(
            final String applicationId,
            final String principal,
            final String ingressTopic,
            final String egressTopic,
            final List<String> removedTags,
            final List<String> addedTags,
            final List<String> operators) {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId(applicationId);
        doc.setPrincipal(principal);

        final ProcessingPolicyDocument.SinkBinding sink = new ProcessingPolicyDocument.SinkBinding();
        sink.setEgressTopic(egressTopic);
        sink.setIngressTopics(List.of(ingressTopic));
        sink.setRemovedTags(removedTags);
        sink.setAddedTags(addedTags);
        sink.setOperators(operators);
        sink.setCallbackProjections(manifestCallbacksForOperators(operators));
        doc.setSinkBindings(List.of(sink));
        return doc;
    }

    private static List<ProcessingPolicyDocument.CallbackProjectionBinding> manifestCallbacksForOperators(
            final List<String> operators) {
        final List<ProcessingPolicyDocument.CallbackProjectionBinding> callbacks = new ArrayList<>();
        for (final String operator : operators) {
            final ProcessingPolicyDocument.CallbackProjectionBinding binding =
                    new ProcessingPolicyDocument.CallbackProjectionBinding();
            binding.setOperator(operator);
            if (JugPipelineProjections.isPipelineProjectionOperator(operator)) {
                binding.setOutputFields(JugPipelineProjections.outputFieldsForOperator(operator));
            }
            callbacks.add(binding);
        }
        return callbacks;
    }

    public static List<String> manifestOperatorsFor(final String principal, final String egressTopic) {
        if (principal == null || egressTopic == null) {
            return List.of();
        }
        final ProcessingPolicyDocument doc =
                switch (principal) {
                    case "fraud-svc" -> fraudValidator();
                    case "inventory-svc" -> inventoryValidator();
                    case "order-details-svc" -> orderDetailsValidator();
                    case "validations-agg-svc" -> validationsAggregator();
                    case "email-svc" -> emailConsumer();
                    default -> null;
                };
        if (doc == null || doc.getSinkBindings() == null) {
            return List.of();
        }
        for (final ProcessingPolicyDocument.SinkBinding binding : doc.getSinkBindings()) {
            if (egressTopic.equals(binding.getEgressTopic()) && binding.getOperators() != null) {
                return binding.getOperators();
            }
        }
        return List.of();
    }

    public static List<AppProcessingPolicy.OperatorCallbackProjection> manifestCallbackProjectionsFor(
            final String principal,
            final String egressTopic) {
        if (principal == null || egressTopic == null) {
            return List.of();
        }
        final ProcessingPolicyDocument doc =
                switch (principal) {
                    case "fraud-svc" -> fraudValidator();
                    case "inventory-svc" -> inventoryValidator();
                    case "order-details-svc" -> orderDetailsValidator();
                    case "validations-agg-svc" -> validationsAggregator();
                    case "email-svc" -> emailConsumer();
                    case "stock-svc" -> stockRepublisher();
                    case "validation-svc" -> validationRepublisher();
                    case "payment-svc" -> paymentRepublisher();
                    default -> null;
                };
        if (doc == null || doc.getSinkBindings() == null) {
            return List.of();
        }
        for (final ProcessingPolicyDocument.SinkBinding binding : doc.getSinkBindings()) {
            if (!egressTopic.equals(binding.getEgressTopic())
                    || binding.getCallbackProjections() == null) {
                continue;
            }
            final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks = new ArrayList<>();
            for (final ProcessingPolicyDocument.CallbackProjectionBinding manifest :
                    binding.getCallbackProjections()) {
                final AppProcessingPolicy.OperatorCallbackProjection callback =
                        new AppProcessingPolicy.OperatorCallbackProjection();
                callback.setOperator(manifest.getOperator());
                callback.setOutputFields(
                        manifest.getOutputFields() == null ? List.of() : manifest.getOutputFields());
                callbacks.add(callback);
            }
            return callbacks;
        }
        return List.of();
    }
}
