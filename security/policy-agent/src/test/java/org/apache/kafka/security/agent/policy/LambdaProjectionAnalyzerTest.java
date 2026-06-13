package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaProjectionAnalyzerTest {

  @Test
  void analyzesBuilderStyleProjectionFromStaticMapper() throws Exception {
    final Set<String> fields =
        LambdaProjectionAnalyzer.analyzeMethod(
            ProjectionMapper.class.getName(),
            "project",
            "(Lorg/apache/kafka/security/agent/policy/LambdaProjectionAnalyzerTest$SampleOrder;)Lorg/apache/kafka/security/agent/policy/LambdaProjectionAnalyzerTest$SampleOrder;");
    assertTrue(fields.contains("id"));
    assertTrue(fields.contains("status"));
    assertTrue(fields.contains("source"));
  }

  static final class ProjectionMapper {
    private ProjectionMapper() {
    }

    static SampleOrder project(final SampleOrder order) {
      return SampleOrder.builder()
          .id(order.id())
          .status(order.status())
          .source(order.source())
          .build();
    }
  }

  private record SampleOrder(long id, String status, String source) {

    static Builder builder() {
      return new Builder();
    }

    static final class Builder {
      private long id;
      private String status;
      private String source;

      Builder id(final long value) {
        id = value;
        return this;
      }

      Builder status(final String value) {
        status = value;
        return this;
      }

      Builder source(final String value) {
        source = value;
        return this;
      }

      SampleOrder build() {
        return new SampleOrder(id, status, source);
      }
    }
  }
}
