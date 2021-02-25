/*
package kafkatoflink;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

*/
/* 用Flink生产Hfile*//*

public class Application {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        devHeader(conf);
        Job job = Job.getInstance(conf);

        // HDFS 输入
        HadoopInputFormat<LongWritable, Text> hadoopIF =
                new HadoopInputFormat<LongWritable, Text>(
                        new TextInputFormat(), LongWritable.class, Text.class, job
                );
        TextInputFormat.addInputPath(job, new Path("hdfs://node1:9000/input/1"));


        // Flink就干了这点事
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<LongWritable, Text>> textDataSet = env.createInput(hadoopIF);
        DataSet<Tuple2<ImmutableBytesWritable,Cell>> ds =  textDataSet.map(v-> Tuple1.of(v.f1.toString()))
                //.returns(Tuple1<String>)
                .groupBy(0)
                .sortGroup(0,Order.ASCENDING)
                .reduceGroup(new createHfile());

        // 设置输出类型
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        // 输出到HDFS
        HadoopOutputFormat<ImmutableBytesWritable, Cell> hadoopOF =
                new HadoopOutputFormat<ImmutableBytesWritable, Cell>(
                        new HFileOutputFormat2(), job
                );
        HFileOutputFormat2.setOutputPath(job, new Path("hdfs://10.111.32.165:8020/user/zhao/out0226/9/"));
        job.setOutputFormatClass(HFileOutputFormat2.class);
        ds.output(hadoopOF);
        env.execute();
    }

    // 生产 Tuple2<ImmutableBytesWritable,Cell>
    public static final class createHfile extends RichGroupReduceFunction<Tuple1<String>, Tuple2<ImmutableBytesWritable,Cell>> {

        @Override
        public void reduce(Iterable<Tuple1<String>> values, Collector<Tuple2<ImmutableBytesWritable, Cell>> out) throws Exception {
            String family="datasfamily";
            String column="content";
            for (Tuple1<String> key:values) {
                ImmutableBytesWritable rowkey = new ImmutableBytesWritable(key.toString().getBytes());
                KeyValue kv = new KeyValue(key.toString().getBytes(), family.getBytes(), column.getBytes() , key.f0.getBytes());
                out.collect(Tuple2.of(rowkey,kv));
            }
        }
    }

    */
/**
     * 本地或测试环境使用
     * @param conf Configuration
     *//*

    private static void devHeader(Configuration conf){
        // 本地测试提交到测试集群
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.job.ubertask.enable", "true");
        conf.set("fs.defaultFS","hdfs://2.2.2.2:8020");
        // 支持hdfs下目录含子目录
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        System.setProperty("hadoop.home.dir", "D:\\soft\\developsoft\\Hadoop\\hadoop-2.6.5");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
    }
}


*/
