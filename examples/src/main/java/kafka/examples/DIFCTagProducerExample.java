package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.CreateTagResponseData;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DIFCTagProducerExample
{
    public static void main(String[] args) throws Exception
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "customerApp");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props))
        {

            // Test 1: Create tag
            System.out.println("=== Testing createTag ===");
            CreateTagResponseData resp1 = producer.createTag("security-tag");
            System.out.println("Error message here is : " + resp1.errorMessage());

            // Test 2: Send regular message (verify producer still works)
            System.out.println("\n=== Testing regular produce ===");
            producer.send(new ProducerRecord<>("test-topic", "key1", "value1 with tagId=" + resp1.tagId()));
            System.out.println("✅ SUCCESS: Regular produce works");

            // Test 3: Try duplicate tag (should fail)
            System.out.println("\n=== Testing duplicate tag (should fail) ===");
            try
            {
                CreateTagResponseData resp2 = producer.createTag("security-tag");
                System.out.println("Error message here is : " + resp2.errorMessage());
            }
            catch (Exception e)
            {
                System.out.println("✅ EXPECTED: Duplicate tag failed: " + e.getMessage());
            }

        }
        catch(Exception e)
        {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
