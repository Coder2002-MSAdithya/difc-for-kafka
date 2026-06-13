package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessorOutputAnalyzerTest {

  @Test
  void infersValidationFieldsFromProcessorGenerics() {
    final Set<String> fields =
        ProcessorOutputAnalyzer.outputFieldsFromProcessorClass(FakeInventoryValidator.class);
    assertTrue(fields.contains("orderId"));
    assertTrue(fields.contains("checkType"));
    assertTrue(fields.contains("validationResult"));
  }

  @Test
  void infersValidationFieldsFromProcessorSupplierGet() {
    final Set<String> fields = ProcessorOutputAnalyzer.analyzeProcessorSupplier(new FakeSupplier());
    assertTrue(fields.contains("orderId"));
    assertTrue(fields.contains("checkType"));
    assertTrue(fields.contains("validationResult"));
  }

  interface FakeProcessor<K, V, KOut, VOut> {
  }

  static final class FakeInventoryValidator implements FakeProcessor<String, String, String, SampleValidation> {
  }

  static final class SampleValidation {
    private String orderId;
    private String checkType;
    private String validationResult;
  }

  static final class FakeSupplier {
    public FakeInventoryValidator get() {
      return new FakeInventoryValidator();
    }
  }
}
