package kafkatoflink;



import Hbase.*;
import entity.clearBean;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class KafkaMessageStreaming4Mysql {

        private static clearBean detail=new clearBean();
        public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


            env.enableCheckpointing(1000);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.setParallelism(10);

            Properties props = new Properties();
            props.setProperty("bootstrap.servers", "10.136.64.2:9092,10.136.64.3:9092,10.136.64.4:9092");
            props.setProperty("group.id", "metric1-group1");
            props.setProperty("auto.offset.reset","earliest");
            props.setProperty("key.deserializer", StringDeserializer.class.getName());
            props.setProperty("value.deserializer", StringDeserializer.class.getName());
            System.out.println("Start!");


            FlinkKafkaConsumer010<String> consumer=new FlinkKafkaConsumer010<>("balance",new SimpleStringSchema(), props);
            DataStream<List<clearBean>> keyedStream = env
                    .addSource(consumer)
                    .flatMap(new FlatMapFunction<String, clearBean>() {
                        @Override
                        public void flatMap(String s, Collector<clearBean> collector) throws Exception {
                            if (s != null && s.contains(",")&&!s.contains("CUSTOMER_ID")) {
                                s=s.replace(":","");
                                String[] parts = s.split(",");
                                detail.setCst_no(Integer.parseInt(parts[1]));
                                detail.setEvent_no(parts[0]);
                                detail.setBusi_code(parts[2]);
                                detail.setClear_total(new BigDecimal(parts[3]));
                                detail.setDate(parts[4]);
                                detail.setTime(parts[5]);
                                collector.collect(detail);
                            }
                        }
                    }).timeWindowAll(Time.seconds(20))
                    .trigger(new CountTrigger(100000))
                    .apply(new AllWindowFunction<clearBean, List<clearBean>, Window>() {
                        @Override
                        public void apply(Window window, Iterable<clearBean> values, Collector<List<clearBean>> out) throws Exception {
                            List<clearBean> lists=new ArrayList<>();
                            for (clearBean value : values) {
                                lists.add(value);
                            }
                            out.collect(lists);
                        }
                    });
            try {
                keyedStream.addSink(new MysqlSinBalance());
            }catch (Exception e){
                e.printStackTrace();
            }
            env.execute("Kafka-Flink Test");
        }

}


