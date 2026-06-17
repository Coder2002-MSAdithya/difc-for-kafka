package org.apache.kafka.security.agent.policy;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Demo topic → Avro record field catalog for static field-sanitization analysis.
 */
public final class TopicSchemaCatalog {

  private static final Set<String> ORDER_SENSITIVE_FIELDS =
      Set.of("customerId", "product", "quantity", "price", "state");

  private static final Set<String> JUG_ORDER_SENSITIVE_FIELDS =
      Set.of("customerId", "productId", "amount", "price");

  private static final Set<String> JUG_CARD_SENSITIVE_FIELDS = Set.of("cardNumber");

  private static final Map<String, Set<String>> TOPIC_VALUE_FIELDS =
      Map.ofEntries(
          Map.entry(
              "orders",
              Set.of("id", "customerId", "state", "product", "quantity", "price")),
          Map.entry(
              "order-validations",
              Set.of("orderId", "checkType", "validationResult")),
          Map.entry("warehouse-inventory", Set.of("quantity")),
          Map.entry(
              "orders-enriched",
              Set.of("id", "customerId", "state", "product", "quantity", "price")),
          Map.entry("payments", Set.of("id", "status", "source")),
          Map.entry("stock", Set.of("id", "status", "source")),
          Map.entry("customers", Set.of("id", "name", "phone")),
          Map.entry(
              JugPipelineProjections.TOPIC_ORDER,
              Set.of("customerId", "productId", "amount", "price", "cardNumber")),
          Map.entry(
              JugPipelineProjections.TOPIC_STOCK_CHECK,
              Set.of("customerId", "productId", "amount", "inStock", "cardNumber")),
          Map.entry(
              JugPipelineProjections.TOPIC_VALIDATION,
              Set.of("customerId", "isNumberValid")),
          Map.entry(
              JugPipelineProjections.TOPIC_BILLING,
              Set.of("customerId", "price")));

  private TopicSchemaCatalog() {
  }

  public static Set<String> valueFieldsForTopic(final String topic) {
    if (topic == null) {
      return Set.of();
    }
    return TOPIC_VALUE_FIELDS.getOrDefault(topic, Collections.emptySet());
  }

  public static Set<String> sensitiveOrderFields() {
    return ORDER_SENSITIVE_FIELDS;
  }

  public static Set<String> sensitiveJugOrderFields() {
    return JUG_ORDER_SENSITIVE_FIELDS;
  }

  public static Set<String> sensitiveCardFields() {
    return JUG_CARD_SENSITIVE_FIELDS;
  }

  public static boolean isKnownTopic(final String topic) {
    return TOPIC_VALUE_FIELDS.containsKey(topic);
  }

  public static Set<String> copyFields(final Set<String> fields) {
    return new LinkedHashSet<>(fields);
  }
}
