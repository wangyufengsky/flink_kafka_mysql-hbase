package Producer;

import entity.Detail;
import kafkatoflink.MemoryUsageExtrator;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Properties;

/**
 * @Description    KafkaProducerTest 发送Kafka消息
 */
public class KafkaProducerTest {

    //private static String filePath="hdfs://node1:9000/input/1";
    //private static String filePath="D:\\big\\52";
    private static String filePath="/apps/big/52";
    private static int nums=0;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        //props.put("bootstrap.servers", "10.134.73.75:9092");
        props.put("bootstrap.servers", "10.141.61.192:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);


        try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(filePath),"UTF-8"))) {
            String line=null;
            while ((line = br.readLine()) != null){
                String[] str=line.split(",");
                if (line != null && line.contains(",")) {
                    producer.send(new ProducerRecord<>("detail6", line), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                System.out.println("Failed to send message with exception " + exception);
                            }
                        }
                    });
                    /*producer.send(new ProducerRecord<>("total4", line), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                System.out.println("Failed to send message with exception " + exception);
                            }
                        }
                    });*/
                    nums++;
                }
                if(nums%5000==0){
                    System.out.println("已经读取"+nums+"条数据到kafka");
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        producer.close();
    }






    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }
}
