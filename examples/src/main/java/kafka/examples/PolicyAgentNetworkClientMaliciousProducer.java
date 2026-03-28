package kafka.examples;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Properties;

/**
 * Malicious test app that uses lower-level Kafka networking classes directly.
 *
 * Expected behavior (with -javaagent):
 * - Connection attempt should be denied when no approved trusted entrypoint is in the active call path.
 */
public final class PolicyAgentNetworkClientMaliciousProducer {
    private PolicyAgentNetworkClientMaliciousProducer() {
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;

        Node node = new Node(0, host, port);
        Metrics metrics = new Metrics();
        NetworkClient client = null;

        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "policy-agent-networkclient-malicious");

            ProducerConfig config = new ProducerConfig(props);
            ManualMetadataUpdater metadataUpdater = new ManualMetadataUpdater(List.of(node));
            LogContext logContext = new LogContext("[policy-agent-networkclient-malicious] ");
            ApiVersions apiVersions = new ApiVersions();

            client = ClientUtils.createNetworkClient(
                    config,
                    "policy-agent-networkclient-malicious",
                    metrics,
                    "producer",
                    logContext,
                    apiVersions,
                    Time.SYSTEM,
                    1,
                    30_000,
                    metadataUpdater,
                    new DefaultHostResolver()
            );

            long now = Time.SYSTEM.milliseconds();
            client.ready(node, now);
            client.poll(1_000, now);
            System.out.println("Unexpectedly completed low-level NetworkClient connect flow.");
        } catch (SecurityException se) {
            System.err.println("Expected policy-agent denial for low-level NetworkClient usage: " + se.getMessage());
        } catch (Exception e) {
            System.err.println("Low-level NetworkClient malicious app encountered non-policy error: " + e.getMessage());
            e.printStackTrace(System.err);
        } finally {
            if (client != null) {
                client.close();
            }
            metrics.close();
        }
    }
}