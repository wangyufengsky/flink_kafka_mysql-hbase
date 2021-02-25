package Producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class kafkaConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.136.64.151:9092,10.136.64.152:9092,10.136.64.153:9092");
        props.setProperty("group.id", "metric-group");
        props.setProperty("auto.offset.reset","earliest");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("balance"));
        ConsumerRecords<String,String> list=consumer.poll(1000);
        if(!list.isEmpty()&&list.count()>0){
            for(ConsumerRecord<String,String> consumerRecord:list){
                System.out.println(consumerRecord.value());
            }
        }
    }
}
