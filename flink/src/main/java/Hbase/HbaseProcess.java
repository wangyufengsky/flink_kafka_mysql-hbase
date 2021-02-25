package Hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.math.BigDecimal;


@Slf4j
public class HbaseProcess extends ProcessFunction<Tuple5<String, String, String, BigDecimal, String>, String> {
    private static final long serialVersionUID = 1L;
    private Configuration configuration=null;
    private Connection connection = null;
    private Table table = null;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        try {
            // 加载HBase的配置
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "10.137.8.22,10.137.8.25,10.137.8.26,10.137.8.34,10.137.8.35,10.137.8.36,10.137.8.38,10.137.8.39,10.137.8.40," +
                    "10.141.13.13,10.141.13.14,10.141.13.15,10.141.13.16,10.141.13.17,10.141.13.18,10.141.13.19,10.141.13.20,10.141.13.21");
            configuration.set("fs.defaultFS", "hdfs://node1:9000");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200");
            connection = ConnectionFactory.createConnection(configuration);
            TableName tableName = TableName.valueOf("tbyjttotal1");
            // 获取表对象
            table = connection.getTable(tableName);
        } catch (Exception e) {
        }
    }

    @Override
    public void close() throws Exception {
        if (null != table) table.close();
        if (null != connection) connection.close();
    }


    @Override
    public void processElement(Tuple5<String, String, String, BigDecimal, String> value, Context ctx, Collector<String> out) throws Exception {
        Job job=new Job(configuration,"HFile bulkload");
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        job.setReducerClass(HFileCreateJob.HfileReducer.class);
        job.setMapperClass(HFileMapper.class);
        RegionLocator regionLocator = connection.getRegionLocator(table.getName());
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(configuration);
        //bulkLoader.doBulkLoad(new Path(hdfsFilePath), admin, table, regionLocator);
    }


    public static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split(",", -1);
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(items[0].getBytes());
            KeyValue kv = new KeyValue(Bytes.toBytes(items[0]),Bytes.toBytes(items[1]), Bytes.toBytes(items[2]),System.currentTimeMillis(), Bytes.toBytes(items[3]));
            if (null != kv) {
                context.write(rowkey, kv);
            }
        }
    }


}

