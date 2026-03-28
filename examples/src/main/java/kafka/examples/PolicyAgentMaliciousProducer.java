package kafka.examples;

import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Malicious test app for the policy agent.
 *
 * Expected behavior (with -javaagent):
 * - Direct socket connect is denied because the call path does not pass through trusted Kafka jars.
 */
public final class PolicyAgentMaliciousProducer {
    private PolicyAgentMaliciousProducer() {
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2_000);
            System.out.println("Unexpectedly connected to " + host + ":" + port);
        } catch (SecurityException se) {
            System.err.println("Expected policy-agent denial: " + se.getMessage());
        } catch (Exception e) {
            System.err.println("Malicious producer encountered non-policy error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}