package kafkatoflink;

/*import Hbase.HbaseSink2;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;*/

public class Application2 {/*

    @SuppressWarnings(value={"unchecked"})
    public static void main(String[] args) throws Exception {
        // kafka 需要的参数
        String brokers = "127.0.0.1:9092";
        String username = "admin";
        String password = "123456";
        String topic = "test";
        // hbase 需要的参数
        String hbase_zookeeper_host = "hbase";
        String hbase_zookeeper_port = "2181";

        // 接收命令行参数，覆盖默认值
        Options options = new Options();
        options.addOption("kafka_brokers", true, "kafka cluster hosts, such 127.0.0.1:9092");
        options.addOption("kafka_username", true, "kafka cluster username, default: admin");
        options.addOption("kafka_user_password", true, "kafka cluster user password, default: 123456");
        options.addOption("kafka_topic", true, "kafka cluster topic, default: test");

        options.addOption("hbase_zookeeper_host", true, "hbase zookeeper host, default: hbase");
        options.addOption("hbase_zookeeper_port", true, "hbase zookeeper port, default: 2181");

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse( options, args );

        if ( line.hasOption( "kafka_brokers" ) ) {
            brokers = line.getOptionValue("kafka_brokers");
        } else {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "flink write hbase job", options );
            System.exit(1);
        }

        if ( line.hasOption( "kafka_username" ) ) {
            username = line.getOptionValue("kafka_username");
        }
        if ( line.hasOption( "kafka_user_password" ) ) {
            password = line.getOptionValue("kafka_user_password");
        }
        if ( line.hasOption( "kafka_topic" ) ) {
            topic = line.getOptionValue("kafka_topic");
        }
        if ( line.hasOption( "hbase_zookeeper_host" ) ) {
            hbase_zookeeper_host = line.getOptionValue("hbase_zookeeper_host");
        }
        if ( line.hasOption( "hbase_zookeeper_port" ) ) {
            hbase_zookeeper_port = line.getOptionValue("hbase_zookeeper_port");
        }

        // 执行任务
        doExcute(brokers, username, password, topic, hbase_zookeeper_host, hbase_zookeeper_port);
    }

*
     * 具体任务执行


    public static void doExcute(String kafka_brokers, String kafka_username, String kafka_password,
                                String topic, String hbase_zookeeper_host, String hbase_zookeeper_port) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 kafka source
        env.enableCheckpointing(5000 * 100000);

        Properties props = getKafkaProperties(kafka_username, kafka_password);
        props.setProperty("bootstrap.servers", kafka_brokers);
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010(topic, new SimpleStringSchema(), props));

        // 过滤不标准格式的数据，并格式化
        DataStream<HttpDataModel> formated_stream = stream.filter(s -> {
            JSONObject obj = JSONObject.parseObject(s);
            return obj.containsKey("project") && obj.containsKey("table") && obj.containsKey("data");
        }).map(s -> { return JSON.parseObject(s, HttpDataModel.class); });

        // 在 10 秒的时间窗口内，每 100 条触发输出到 hbase
        DataStream<List<HttpDataModel>> batch_stream = formated_stream
                .timeWindowAll(Time.seconds(10))
                .trigger(new CountTrigger(100))
                .apply(new AllWindowFunction<HttpDataModel, List<HttpDataModel>, Window>() {
                    public void apply(Window window, Iterable<HttpDataModel> values, Collector<List<HttpDataModel>> out) throws Exception {
                        List<HttpDataModel> lists = new ArrayList<HttpDataModel>();
                        for (HttpDataModel value : values) {
                            lists.add(value);
                        }
                        out.collect(lists);
                    }
                });

        batch_stream.addSink(new HbaseSink2());

        // 控制台输出
        //batch_stream.print();

        env.execute("integration-http");
    }

*
     * 获取 kafka 的默认配置


    public static Properties getKafkaProperties(String username, String password) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("group.id", "dataworks-integration");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, password);

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", jaasCfg);
        return props;
    }
*/}

