import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import model.Activity;

public class ActivityProducer {
    private static final String TOPIC_NAME = "vdt2024";
    private static final String BOOTSTRAP_SERVERS = "localhost:9094";

    public static void main(String[] args) {
        try {
//            Defining props and set up Kafka Producer
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            props.put("schema.registry.url", "http://localhost:8081");
            props.put("value.subject.name.strategy", RecordNameStrategy.class.getName());
            KafkaProducer<String, Activity> producer = new KafkaProducer<>(props);

//            Get the csv file and parse it
            InputStream inputStream = ActivityProducer.class.getResourceAsStream("/log_action.csv");
            if (inputStream != null) {
                try (InputStreamReader reader = new InputStreamReader(inputStream)) {
                    CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
                    for (CSVRecord csvRecord : csvParser) {
                        int studentCode = Integer.parseInt(csvRecord.get(0));
                        String activity = csvRecord.get(1);
                        int numberOfFiles = Integer.parseInt(csvRecord.get(2));
                        String timeStamp = csvRecord.get(3);
                        Activity currentActivity = Activity.newBuilder().setStudentCode(studentCode).setActivity(activity).setNumberOfFiles(numberOfFiles).setTimestamp(timeStamp).build();
//                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, csvRecord.get(0) + "," + csvRecord.get(1));
                        ProducerRecord<String, Activity> record = new ProducerRecord<>(TOPIC_NAME, currentActivity);
                        producer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                System.out.println("Send record: " + record.value());
                            } else {
                                System.out.println("Error sending record: " + record.value());
                                System.out.println(exception.getMessage());
                            }
                        });

                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}