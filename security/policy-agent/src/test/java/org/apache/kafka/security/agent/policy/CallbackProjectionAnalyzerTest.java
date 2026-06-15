package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CallbackProjectionAnalyzerTest {

  interface FakeProcessor<K, V, KOut, VOut> {
  }

  static final class SampleValidation {
    private String orderId;
    private String checkType;
    private String validationResult;
  }

  static final class FakeInventoryValidator implements FakeProcessor<String, String, String, SampleValidation> {
  }

  static final class FakeSupplier {
    public FakeInventoryValidator get() {
      return new FakeInventoryValidator();
    }
  }

  @Test
  void infersProcessOutputFromProcessorSupplier() {
    assertTrue(
        CallbackProjectionAnalyzer.analyze("process", new FakeSupplier()).contains("orderId"));
  }
}
