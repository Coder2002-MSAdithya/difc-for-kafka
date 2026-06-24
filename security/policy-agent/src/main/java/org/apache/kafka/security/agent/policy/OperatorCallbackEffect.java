package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Relational-algebra effect inferred from one DSL operator callback (π, σ, π_k, γ, …). */
public final class OperatorCallbackEffect {

  private final Set<String> outputFields;
  private final Set<String> selectionFields;
  private final String selectionExpression;
  private final Set<String> keyFields;
  private final List<FieldLineage> fieldLineages;

  private OperatorCallbackEffect(
      final Set<String> outputFields,
      final Set<String> selectionFields,
      final String selectionExpression,
      final Set<String> keyFields,
      final List<FieldLineage> fieldLineages) {
    this.outputFields = copy(outputFields);
    this.selectionFields = copy(selectionFields);
    this.selectionExpression = selectionExpression == null ? "" : selectionExpression;
    this.keyFields = copy(keyFields);
    this.fieldLineages = fieldLineages == null ? List.of() : List.copyOf(fieldLineages);
  }

  public static OperatorCallbackEffect empty() {
    return new OperatorCallbackEffect(Set.of(), Set.of(), "", Set.of(), List.of());
  }

  public static OperatorCallbackEffect projection(final Set<String> outputFields) {
    return new OperatorCallbackEffect(outputFields, Set.of(), "", Set.of(), List.of());
  }

  public static OperatorCallbackEffect projectionWithLineage(final List<FieldLineage> lineages) {
    final LinkedHashSet<String> names = new LinkedHashSet<>();
    if (lineages != null) {
      for (final FieldLineage lineage : lineages) {
        if (lineage != null && lineage.getOutputField() != null && !lineage.getOutputField().isEmpty()) {
          names.add(lineage.getOutputField());
        }
      }
    }
    return new OperatorCallbackEffect(names, Set.of(), "", Set.of(), lineages);
  }

  public static OperatorCallbackEffect selection(
      final Set<String> selectionFields, final String selectionExpression) {
    return new OperatorCallbackEffect(Set.of(), selectionFields, selectionExpression, Set.of(), List.of());
  }

  public static OperatorCallbackEffect selectionWithLineage(
      final Set<String> selectionFields,
      final String selectionExpression,
      final FieldLineage predicateLineage) {
    return new OperatorCallbackEffect(
        Set.of(),
        selectionFields,
        selectionExpression,
        Set.of(),
        predicateLineage == null ? List.of() : List.of(predicateLineage));
  }

  public static OperatorCallbackEffect keyProjection(final Set<String> keyFields) {
    return new OperatorCallbackEffect(Set.of(), Set.of(), "", keyFields, List.of());
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

  public List<FieldLineage> fieldLineages() {
    return fieldLineages;
  }

  public boolean isEmpty() {
    return outputFields.isEmpty()
        && selectionFields.isEmpty()
        && selectionExpression.isEmpty()
        && keyFields.isEmpty()
        && fieldLineages.isEmpty();
  }

  private static Set<String> copy(final Set<String> fields) {
    return fields == null || fields.isEmpty() ? Set.of() : Set.copyOf(fields);
  }

  public static final class Builder {
    private Set<String> outputFields = new LinkedHashSet<>();
    private Set<String> selectionFields = new LinkedHashSet<>();
    private String selectionExpression = "";
    private Set<String> keyFields = new LinkedHashSet<>();
    private List<FieldLineage> fieldLineages = new ArrayList<>();

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

    public Builder fieldLineages(final List<FieldLineage> lineages) {
      this.fieldLineages = lineages == null ? new ArrayList<>() : new ArrayList<>(lineages);
      return this;
    }

    public OperatorCallbackEffect build() {
      return new OperatorCallbackEffect(
          outputFields, selectionFields, selectionExpression, keyFields, fieldLineages);
    }
  }
}
