package org.apache.kafka.security.agent.policy;

import org.apache.kafka.security.agent.DslProcessingPolicyTracker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Debounced re-sign of processing-policy.json when runtime field facts change. */
public final class PolicyRefreshScheduler {

  private static final ScheduledExecutorService EXECUTOR =
      Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "policy-refresh-scheduler");
        t.setDaemon(true);
        return t;
      });
  private static final AtomicBoolean REFRESH_PENDING = new AtomicBoolean(false);

  private PolicyRefreshScheduler() {
  }

  public static void scheduleRefresh() {
    if (REFRESH_PENDING.compareAndSet(false, true)) {
      EXECUTOR.schedule(PolicyRefreshScheduler::runRefresh, 250, TimeUnit.MILLISECONDS);
    }
  }

  private static void runRefresh() {
    REFRESH_PENDING.set(false);
    DslProcessingPolicyTracker.writePolicyJsonFile();
  }
}
