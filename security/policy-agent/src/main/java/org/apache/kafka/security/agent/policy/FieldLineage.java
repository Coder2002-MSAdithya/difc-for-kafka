package org.apache.kafka.security.agent.policy;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Provenance of one output column: which input fields it derives from, how it was computed, and
 * whether that computation sanitizes sensitive grantor data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class FieldLineage {

  public enum SanitizationKind {
    /** Value is a direct copy of a source column (same semantics). */
    PASSTHROUGH,
    /** Boolean result of a comparison/predicate on sources (e.g. {@code price >= 50}). */
    BOOLEAN_PREDICATE,
    /** Survives only as an aggregate/grouped result, not as a per-record scalar. */
    AGGREGATE,
    /** Used only for grouping/window key, not emitted as a value column. */
    KEY_ONLY,
    /** Literal/constant with no tagged input sources. */
    CONSTANT,
    /** Computed expression that may still leak sensitive information (e.g. {@code price * 1.1}). */
    DERIVED,
    /** Column dropped from the relation. */
    ABSENT;

    public static SanitizationKind parse(final String raw) {
      if (raw == null || raw.isEmpty()) {
        return PASSTHROUGH;
      }
      try {
        return valueOf(raw.toUpperCase(Locale.ROOT));
      } catch (final IllegalArgumentException ignored) {
        return PASSTHROUGH;
      }
    }
  }

  public enum ValueType {
    BOOLEAN,
    INT,
    LONG,
    DOUBLE,
    STRING,
    OBJECT,
    AGGREGATE,
    UNKNOWN;

    public static ValueType parse(final String raw) {
      if (raw == null || raw.isEmpty()) {
        return UNKNOWN;
      }
      try {
        return valueOf(raw.toUpperCase(Locale.ROOT));
      } catch (final IllegalArgumentException ignored) {
        return UNKNOWN;
      }
    }

    public static ValueType fromDescriptor(final String desc) {
      if (desc == null || desc.isEmpty()) {
        return UNKNOWN;
      }
      return switch (desc) {
        case "Z" -> BOOLEAN;
        case "I" -> INT;
        case "J" -> LONG;
        case "D" -> DOUBLE;
        case "F" -> DOUBLE;
        default -> desc.startsWith("L") && desc.endsWith(";") ? OBJECT : UNKNOWN;
      };
    }
  }

  private String outputField = "";
  private String valueType = ValueType.UNKNOWN.name();
  private List<String> sourceFields = new ArrayList<>();
  private String expression = "";
  private String sanitizationKind = SanitizationKind.PASSTHROUGH.name();

  public FieldLineage() {}

  public FieldLineage(
      final String outputField,
      final ValueType valueType,
      final Set<String> sourceFields,
      final String expression,
      final SanitizationKind sanitizationKind) {
    this.outputField = outputField == null ? "" : outputField;
    this.valueType = valueType == null ? ValueType.UNKNOWN.name() : valueType.name();
    this.sourceFields = sourceFields == null ? new ArrayList<>() : new ArrayList<>(sourceFields);
    this.expression = expression == null ? "" : expression;
    this.sanitizationKind =
        sanitizationKind == null ? SanitizationKind.PASSTHROUGH.name() : sanitizationKind.name();
  }

  public static FieldLineage passthrough(final String field, final ValueType type) {
    return new FieldLineage(
        field,
        type,
        Set.of(field),
        field,
        SanitizationKind.PASSTHROUGH);
  }

  public static FieldLineage aggregateOf(final String outputField, final Set<String> sources, final String expression) {
    return new FieldLineage(
        outputField,
        ValueType.AGGREGATE,
        sources,
        expression == null || expression.isEmpty() ? "γ(" + String.join(", ", sources) + ")" : expression,
        SanitizationKind.AGGREGATE);
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(final String outputField) {
    this.outputField = outputField == null ? "" : outputField;
  }

  public String getValueType() {
    return valueType;
  }

  public void setValueType(final String valueType) {
    this.valueType = valueType == null ? ValueType.UNKNOWN.name() : valueType;
  }

  public List<String> getSourceFields() {
    return sourceFields == null ? Collections.emptyList() : sourceFields;
  }

  public void setSourceFields(final List<String> sourceFields) {
    this.sourceFields = sourceFields == null ? new ArrayList<>() : new ArrayList<>(sourceFields);
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(final String expression) {
    this.expression = expression == null ? "" : expression;
  }

  public String getSanitizationKind() {
    return sanitizationKind;
  }

  public void setSanitizationKind(final String sanitizationKind) {
    this.sanitizationKind = sanitizationKind == null ? SanitizationKind.PASSTHROUGH.name() : sanitizationKind;
  }

  public ValueType valueTypeEnum() {
    return ValueType.parse(valueType);
  }

  public SanitizationKind sanitizationKindEnum() {
    return SanitizationKind.parse(sanitizationKind);
  }

  public Set<String> sourceFieldSet() {
    return new LinkedHashSet<>(getSourceFields());
  }

  public boolean sanitizesSensitiveSources() {
    return switch (sanitizationKindEnum()) {
      case BOOLEAN_PREDICATE, AGGREGATE, CONSTANT, KEY_ONLY, ABSENT -> true;
      case PASSTHROUGH, DERIVED -> false;
    };
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FieldLineage that)) {
      return false;
    }
    return Objects.equals(outputField, that.outputField)
        && Objects.equals(valueType, that.valueType)
        && Objects.equals(sourceFields, that.sourceFields)
        && Objects.equals(expression, that.expression)
        && Objects.equals(sanitizationKind, that.sanitizationKind);
  }

  @Override
  public int hashCode() {
    return Objects.hash(outputField, valueType, sourceFields, expression, sanitizationKind);
  }
}
