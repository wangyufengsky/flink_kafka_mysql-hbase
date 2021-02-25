package RelTest.Flinks;


import Hbase.MysqlSinBalance;
import RelTest.InsertBean.ClearTotalInsertBean;
import RelTest.InsertBean.DetailInsertBean;
import RelTest.Sink.DetailSink;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import entity.clearBean;
import kafkatoflink.CountTrigger;
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

import static com.alibaba.fastjson.JSON.toJavaObject;


public class KafkaMessageStreaming4Detail {

        private static DetailInsertBean detail=new DetailInsertBean();
        public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


            env.enableCheckpointing(1000);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.setParallelism(1);

            Properties props = new Properties();
            props.setProperty("bootstrap.servers", "10.136.64.151:9092,10.136.64.152:9092,10.136.64.153:9092");
            props.setProperty("group.id", "beiya");
            props.setProperty("auto.offset.reset","earliest");
            props.setProperty("key.deserializer", StringDeserializer.class.getName());
            props.setProperty("value.deserializer", StringDeserializer.class.getName());
            System.out.println("Start!");


            FlinkKafkaConsumer010<String> consumer=new FlinkKafkaConsumer010<>("beiya",new SimpleStringSchema(), props);
            DataStream<List<DetailInsertBean>> keyedStream = env .addSource(consumer)
                    .flatMap(new FlatMapFunction<String, DetailInsertBean>() {
                        @Override
                        public void flatMap(String json, Collector<DetailInsertBean> collector) throws Exception {
                            if(json!=null){
                                detail=JSONStringtoBean(JSONObject.parseObject(json),"RelTest.InsertBean.DetailInsertBean");
                                collector.collect(detail);
                            }
                        }
                    }).timeWindowAll(Time.seconds(20))
                    .trigger(new CountTrigger(1))
                    .apply(new AllWindowFunction<DetailInsertBean, List<DetailInsertBean>, Window>() {
                        @Override
                        public void apply(Window window, Iterable<DetailInsertBean> values, Collector<List<DetailInsertBean>> out) throws Exception {
                            List<DetailInsertBean> lists=new ArrayList<>();
                            for (DetailInsertBean value : values) {
                                lists.add(value);
                            }
                            out.collect(lists);
                        }
                    });
            try {
                keyedStream.addSink(new DetailSink());
            }catch (Exception e){
                e.printStackTrace();
            }
            env.execute("Kafka-Flink Test");
        }


    private static  <T> T JSONStringtoBean(JSON json, String className) throws Exception{
        return (T)toJavaObject(json,Class.forName(className));
    }

}


