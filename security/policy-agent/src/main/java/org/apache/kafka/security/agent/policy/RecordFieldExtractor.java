package org.apache.kafka.security.agent.policy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Extracts logical field names from produced/consumed record values (Avro, POJO, Map). */
public final class RecordFieldExtractor {

  private static final Set<String> IGNORED_NAMES =
      Set.of("class", "schema", "specificData", "customEncoder", "customDecoder");

  private RecordFieldExtractor() {
  }

  public static Set<String> extractFieldsFromType(final Class<?> type) {
    if (type == null || type == Object.class) {
      return Set.of();
    }
    final Set<String> avroFields = extractAvroFieldsFromClass(type);
    if (!avroFields.isEmpty()) {
      return avroFields;
    }
    return extractPojoFields(type);
  }

  public static Set<String> extractFields(final Object value) {
    if (value == null) {
      return Set.of();
    }
    if (value instanceof Map<?, ?> map) {
      final LinkedHashSet<String> fields = new LinkedHashSet<>();
      for (final Object key : map.keySet()) {
        if (key != null) {
          fields.add(String.valueOf(key));
        }
      }
      return fields;
    }
    final Set<String> avroFields = extractAvroFields(value);
    if (!avroFields.isEmpty()) {
      return avroFields;
    }
    return extractPojoFields(value.getClass());
  }

  /** Returns POJO/Avro fields whose values are non-null on this instance (projection after map). */
  public static Set<String> extractPopulatedFields(final Object value) {
    if (value == null) {
      return Set.of();
    }
    if (value instanceof Map<?, ?> map) {
      final LinkedHashSet<String> fields = new LinkedHashSet<>();
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        if (entry.getKey() != null && entry.getValue() != null) {
          fields.add(String.valueOf(entry.getKey()));
        }
      }
      return fields;
    }
    final Set<String> avroFields = extractAvroFields(value);
    if (!avroFields.isEmpty()) {
      final LinkedHashSet<String> populated = new LinkedHashSet<>();
      for (final String field : avroFields) {
        if (readProperty(value, field) != null) {
          populated.add(field);
        }
      }
      return populated;
    }
    final LinkedHashSet<String> populated = new LinkedHashSet<>();
    for (final String field : extractPojoFields(value.getClass())) {
      final Object property = readProperty(value, field);
      if (property == null) {
        continue;
      }
      if (property instanceof Number number && number.longValue() == 0L) {
        continue;
      }
      if (property instanceof Boolean bool && !bool) {
        continue;
      }
      populated.add(field);
    }
    return populated;
  }

  private static Object readProperty(final Object value, final String field) {
    final String getter = "get" + Character.toUpperCase(field.charAt(0)) + field.substring(1);
    try {
      final Method method = value.getClass().getMethod(getter);
      return method.invoke(value);
    } catch (final ReflectiveOperationException ignored) {
      try {
        final Field declared = value.getClass().getDeclaredField(field);
        if (declared.trySetAccessible()) {
          return declared.get(value);
        }
      } catch (final ReflectiveOperationException ignoredAgain) {
        return null;
      }
    }
    return null;
  }

  private static Set<String> extractAvroFields(final Object value) {
    try {
      final Method getSchema = value.getClass().getMethod("getSchema");
      final Object schema = getSchema.invoke(value);
      return fieldNamesFromAvroSchema(schema);
    } catch (final ReflectiveOperationException e) {
      return Set.of();
    }
  }

  private static Set<String> extractAvroFieldsFromClass(final Class<?> type) {
    try {
      final Method getClassSchema = type.getMethod("getClassSchema");
      final Object schema = getClassSchema.invoke(null);
      return fieldNamesFromAvroSchema(schema);
    } catch (final ReflectiveOperationException ignored) {
      return Set.of();
    }
  }

  private static Set<String> fieldNamesFromAvroSchema(final Object schema) {
    if (schema == null) {
      return Set.of();
    }
    try {
      final Method getFields = schema.getClass().getMethod("getFields");
      final Object fieldsObj = getFields.invoke(schema);
      if (!(fieldsObj instanceof List<?> fields)) {
        return Set.of();
      }
      final LinkedHashSet<String> names = new LinkedHashSet<>();
      for (final Object field : fields) {
        if (field == null) {
          continue;
        }
        final Method nameMethod = field.getClass().getMethod("name");
        names.add(String.valueOf(nameMethod.invoke(field)));
      }
      return names;
    } catch (final ReflectiveOperationException e) {
      return Set.of();
    }
  }

  private static Set<String> extractPojoFields(final Class<?> type) {
    final LinkedHashSet<String> fields = new LinkedHashSet<>();
    for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
      for (final Field field : current.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
          continue;
        }
        final String name = field.getName();
        if (!IGNORED_NAMES.contains(name)) {
          fields.add(name);
        }
      }
    }
    if (!fields.isEmpty()) {
      return fields;
    }
    for (final Method method : type.getMethods()) {
      if (Modifier.isStatic(method.getModifiers()) || method.getParameterCount() != 0) {
        continue;
      }
      final String name = method.getName();
      if (name.startsWith("get") && name.length() > 3 && method.getReturnType() != void.class) {
        fields.add(Character.toLowerCase(name.charAt(3)) + name.substring(4));
      }
    }
    return fields;
  }
}
