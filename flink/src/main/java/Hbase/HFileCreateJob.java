package Hbase;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mybatis.logging.Logger;
import org.mybatis.logging.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class HFileCreateJob {

    public void run(String input,String output,String env) throws Exception {

        Configuration conf = new Configuration();
        if("dev".equals(env)){
            devHeader(conf) ;
        }

        try {
            // 运行前，删除已存在的中间输出目录
            try {
                FileSystem fs = FileSystem.get(URI.create(output), conf);
                fs.delete(new Path(output), true);
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Job job = Job.getInstance(conf, "HFileCreateJob");
            job.setJobName("HFileCreateJob");
            job.setJarByClass(HFileCreateJob.class);
            job.setMapperClass(HfileMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(HfileReducer.class);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(KeyValue.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.setOutputFormatClass(HFileOutputFormat2.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void devHeader(Configuration conf){
        // 本地测试提交到测试集群
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.job.ubertask.enable", "true");
        conf.set("mapreduce.job.jar","E:\\intermult-hbase\\target\\intermulthbase-1.0-SNAPSHOT.jar");
        // 支持hdfs下目录含子目录
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        conf.set("hbase.zookeeper.quorum", "10.137.8.22,10.137.8.25,10.137.8.26,10.137.8.34,10.137.8.35,10.137.8.36,10.137.8.38,10.137.8.39,10.137.8.40," +
                "10.141.13.13,10.141.13.14,10.141.13.15,10.141.13.16,10.141.13.17,10.141.13.18,10.141.13.19,10.141.13.20,10.141.13.21");
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200");

    }

    public class HfileMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String rowKeySalt =  ConfigFactory.load().getConfig("hfileCreate").getString("rowKeySalt") ;

        @Override
        protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            String content = value.toString();
            Text rowKey = new Text(datas[7]+datas[12]) ;
            context.write(rowKey,new Text(content));
        }
    }

    public static class HfileReducer extends Reducer<Text, Text, ImmutableBytesWritable, KeyValue> {
        private Config env =  ConfigFactory.load().getConfig("hfileCreate") ;
        private String family =  env.getString("family") ;
        private String column=  env.getString("column") ;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, KeyValue>.Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                try{
                    String line = value.toString();
                    ImmutableBytesWritable rowkey = new ImmutableBytesWritable(key.toString().getBytes());
                    KeyValue kv = new KeyValue(key.toString().getBytes(), this.family.getBytes(), column.getBytes() , line.getBytes());
                    context.write(rowkey, kv);

                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        }

    }
}
