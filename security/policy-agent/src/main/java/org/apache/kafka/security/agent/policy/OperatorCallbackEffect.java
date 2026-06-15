package org.apache.kafka.security.agent.policy;

import java.util.LinkedHashSet;
import java.util.Set;

/** Relational-algebra effect inferred from one DSL operator callback (π, σ, π_k, …). */
public final class OperatorCallbackEffect {

  private final Set<String> outputFields;
  private final Set<String> selectionFields;
  private final String selectionExpression;
  private final Set<String> keyFields;

  private OperatorCallbackEffect(
      final Set<String> outputFields,
      final Set<String> selectionFields,
      final String selectionExpression,
      final Set<String> keyFields) {
    this.outputFields = copy(outputFields);
    this.selectionFields = copy(selectionFields);
    this.selectionExpression = selectionExpression == null ? "" : selectionExpression;
    this.keyFields = copy(keyFields);
  }

  public static OperatorCallbackEffect empty() {
    return new OperatorCallbackEffect(Set.of(), Set.of(), "", Set.of());
  }

  public static OperatorCallbackEffect projection(final Set<String> outputFields) {
    return new OperatorCallbackEffect(outputFields, Set.of(), "", Set.of());
  }

  public static OperatorCallbackEffect selection(
      final Set<String> selectionFields, final String selectionExpression) {
    return new OperatorCallbackEffect(Set.of(), selectionFields, selectionExpression, Set.of());
  }

  public static OperatorCallbackEffect keyProjection(final Set<String> keyFields) {
    return new OperatorCallbackEffect(Set.of(), Set.of(), "", keyFields);
  }

  public static Builder builder() {
    return new Builder();
  }

  public Set<String> outputFields() {
    return outputFields;
  }

  public Set<String> selectionFields() {
    return selectionFields;
  }

  public String selectionExpression() {
    return selectionExpression;
  }

  public Set<String> keyFields() {
    return keyFields;
  }

  public boolean isEmpty() {
    return outputFields.isEmpty()
        && selectionFields.isEmpty()
        && selectionExpression.isEmpty()
        && keyFields.isEmpty();
  }

  private static Set<String> copy(final Set<String> fields) {
    return fields == null || fields.isEmpty() ? Set.of() : Set.copyOf(fields);
  }

  public static final class Builder {
    private Set<String> outputFields = new LinkedHashSet<>();
    private Set<String> selectionFields = new LinkedHashSet<>();
    private String selectionExpression = "";
    private Set<String> keyFields = new LinkedHashSet<>();

    public Builder outputFields(final Set<String> fields) {
      this.outputFields = fields == null ? new LinkedHashSet<>() : new LinkedHashSet<>(fields);
      return this;
    }

    public Builder selectionFields(final Set<String> fields) {
      this.selectionFields = fields == null ? new LinkedHashSet<>() : new LinkedHashSet<>(fields);
      return this;
    }

    public Builder selectionExpression(final String expression) {
      this.selectionExpression = expression == null ? "" : expression;
      return this;
    }

    public Builder keyFields(final Set<String> fields) {
      this.keyFields = fields == null ? new LinkedHashSet<>() : new LinkedHashSet<>(fields);
      return this;
    }

    public OperatorCallbackEffect build() {
      return new OperatorCallbackEffect(outputFields, selectionFields, selectionExpression, keyFields);
    }
  }
}
