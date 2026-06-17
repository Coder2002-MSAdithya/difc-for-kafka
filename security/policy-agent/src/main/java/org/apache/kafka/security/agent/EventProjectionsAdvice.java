package org.apache.kafka.security.agent;

import net.bytebuddy.asm.Advice;
import org.apache.kafka.security.agent.policy.AppProcessingPolicy;
import org.apache.kafka.security.agent.policy.JugPipelineProjections;

import java.lang.reflect.Method;

/** Captures {@code EventProjections} callbacks used by plain Kafka pipeline services. */
public final class EventProjectionsAdvice {

    private EventProjectionsAdvice() {
    }

    public static class ForStockCheckAdvice {
        @Advice.OnMethodExit
        public static void exit(@Advice.Origin Method method) {
            recordProjection(method);
        }
    }

    public static class ForValidationAdvice {
        @Advice.OnMethodExit
        public static void exit(@Advice.Origin Method method) {
            recordProjection(method);
        }
    }

    public static class ForBillingAdvice {
        @Advice.OnMethodExit
        public static void exit(@Advice.Origin Method method) {
            recordProjection(method);
        }
    }

    private static void recordProjection(final Method method) {
        if (method == null) {
            return;
        }
        final String operator = method.getName();
        if (!JugPipelineProjections.isPipelineProjectionOperator(operator)) {
            return;
        }
        final AppProcessingPolicy.OperatorCallbackProjection projection =
                JugPipelineProjections.callbackForOperator(operator);
        AppClientPolicyTracker.recordProjectionCallback(projection);
    }
}
