// import io.confluent.kafka.serializers.KafkaAvroDeserializer;
// import model.Activity;
// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.apache.kafka.clients.consumer.ConsumerRecords;
// import org.apache.kafka.clients.consumer.KafkaConsumer;
// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.common.serialization.StringDeserializer;

// import java.time.Duration;
// import java.util.Collections;
// import java.util.Properties;

// public class ActivityConsumer {
//     private static final String TOPIC_NAME = "vdt2024";
//     private static final String BOOTSTRAP_SERVERS = "localhost:9094";
//     private static final String GROUP_ID = "activity_consumer_group_1";

//     public static void main(String[] args) {
//         Properties props = new Properties();
//         props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//         props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
//         props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//         props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
//         props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//         props.put("schema.registry.url", "http://localhost:8081");
//         props.put("specific.avro.reader", "true");

//         KafkaConsumer<String, Activity> consumer = new KafkaConsumer<>(props);
//         consumer.subscribe(Collections.singletonList(TOPIC_NAME));

//         System.out.println("Consumer started, subscribing to topic: " + TOPIC_NAME);

//         try {
//             while (true) {
//                 ConsumerRecords<String, Activity> records = consumer.poll(Duration.ofMillis(1000));

//                 if (records.isEmpty()) {
//                     System.out.println("No records found in this poll.");
//                 } else {
//                     System.out.println("Fetched " + records.count() + " records in this poll.");
//                     for (ConsumerRecord<String, Activity> record : records) {
//                         Activity activity = record.value();
//                         System.out.println("CONSUMED: " + activity);
//                     }
//                 }
//                 Thread.sleep(500);
//             }
//         } catch (Exception e) {
//             e.printStackTrace();
//         } finally {
//             consumer.close();
//             System.out.println("Consumer closed.");
//         }
//     }
// }
