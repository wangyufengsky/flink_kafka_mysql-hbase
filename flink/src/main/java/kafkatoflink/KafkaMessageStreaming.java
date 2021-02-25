package kafkatoflink;

import Hbase.HbaseSink4Detail;
import Hbase.HbaseSink4Total;
import entity.Detail;
import entity.Total;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.*;


public class KafkaMessageStreaming {

        //private static Map<String,Total> totals=new HashMap<>();
        private static Detail detail=new Detail();
        public static void main(String[] args) throws Exception {

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // 非常关键，一定要设置启动检查点！！
            env.enableCheckpointing(1000);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.setParallelism(3);

            Properties props = new Properties();
            //props.setProperty("bootstrap.servers", "10.134.73.75:9092");
            props.setProperty("bootstrap.servers", "10.141.61.192:9092");
            props.setProperty("group.id", "flink-group");

            //传入的是kafka中的topic
            FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("detail6", new SimpleStringSchema(), props);
            //consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());
            DataStream<List<Detail>> keyedStream = env
                    .addSource(consumer)
                    .flatMap(new FlatMapFunction<String, Detail>() {
                        @Override
                        public void flatMap(String s, Collector<Detail> collector) throws Exception {
                            if (s != null && s.contains(",")) {
                                String[] parts = s.split(",");
                                //客户号、日期、类型、金额、总消息
                                detail.setType(parts[1]);
                                detail.setMoney(new BigDecimal(parts[5]));
                                detail.setCertNo(parts[7]);
                                detail.setDate(parts[12]);
                                detail.setAll(s);
/*                                if(totals.containsKey(detail.getOnlyone())){
                                    totals.put(detail.getOnlyone(),new Total(detail.getRowKey(),detail.getDate(),detail.getType(),totals.get(detail.getOnlyone()).getTotalAmt().add(detail.getMoney())));
                                }else {
                                    totals.put(detail.getOnlyone(),new Total(detail.getRowKey(),detail.getDate(),detail.getType(),detail.getMoney()));
                                }
                                detail.setTotalAmt(totals.get(detail.getOnlyone()).getTotalAmt());*/
                                collector.collect(detail);
                            }
                        }
                    }).timeWindowAll(Time.seconds(20))
                    .trigger(new CountTrigger(10000))
                    .apply(new AllWindowFunction<Detail, List<Detail>, Window>() {
                        @Override
                        public void apply(Window window, Iterable<Detail> values, Collector<List<Detail>> out) throws Exception {
                            List<Detail> lists = new ArrayList<>();
                            for (Detail value : values) {
                                lists.add(value);
                            }
                            out.collect(lists);
                        }
                    });
            keyedStream.addSink(new HbaseSink4Detail());
            env.execute("Kafka-Flink Test");
        }

}


