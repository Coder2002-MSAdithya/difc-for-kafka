package kafka.examples;

import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.PlaintextChannelBuilder;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.JmxReporter;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PolicyAgentNetworkClientBypass {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: <host> <port>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        try {
            System.out.println("Attempting Kafka Selector-based bypass...");

            Time time = Time.SYSTEM;

            Metrics metrics = new Metrics(
                    new MetricConfig(),
                    Collections.singletonList(new JmxReporter()),
                    time
            );

            LogContext logContext = new LogContext();

            // ✅ Correct ListenerName usage
            ListenerName listenerName =
                    ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);

            ChannelBuilder channelBuilder =
                    new PlaintextChannelBuilder(listenerName);

            // ✅ REQUIRED FIX: configure ChannelBuilder
            Map<String, Object> configs = new HashMap<>();
            channelBuilder.configure(configs);

            Selector selector = new Selector(
                    5000,
                    metrics,
                    time,
                    "bypass-client",
                    channelBuilder,
                    logContext
            );

            InetSocketAddress address = new InetSocketAddress(host, port);

            System.out.println("Connecting to " + address);

            // 🔥 This triggers Socket.connect internally
            selector.connect(
                    "node-1",
                    address,
                    64 * 1024,
                    64 * 1024
            );

            // Force actual network I/O
            selector.poll(1000);

            System.out.println("If you see this, bypass succeeded (BAD).");

        } catch (Exception e) {
            System.err.println("Bypass attempt result: " + e.getMessage());
            e.printStackTrace();
        }
    }
}