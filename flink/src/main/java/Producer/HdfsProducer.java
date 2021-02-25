package Producer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

public class HdfsProducer {
    private static String filePath="hdfs://node1:9000/input/1";
    private static int nums=0;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.141.61.192:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        System.setProperty("hadoop.home.dir","/apps/hadoop-2.7.7/");
        Producer<String, String> producer = new KafkaProducer<>(props);

        Configuration conf=new Configuration();
        URI uri=URI.create(filePath);
        FileSystem hdfs=FileSystem.get(uri,conf);

        try(BufferedReader br=new BufferedReader(new InputStreamReader(new FSDataInputStream(hdfs.open(new Path(filePath))),"UTF-8"))) {
            String line=null;
            while ((line = br.readLine()) != null){
                String[] str=line.split(",");
                if (line != null && line.contains(",")) {
                    String[] parts = line.split(",");
                    producer.send(new ProducerRecord<>("test3", line), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                System.out.println("Failed to send message with exception " + exception);
                            }
                        }
                    });
                    nums++;
                }
                if(nums%1000==0){
                    System.out.println("已经读取"+nums+"条数据到kafka");
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        producer.close();





    }
}
